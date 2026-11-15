import re
from typing import Any, Callable, Optional

import pytest
from sqlalchemy.dialects import mysql, postgresql, sqlite

import ormar
from ormar import F
from ormar.queryset.queryset import QuerySet
from ormar.queryset.write_statements import (
    BulkUpdateWrite,
    DeleteWrite,
    LockWrite,
    RowSelectWrite,
    RowsLockWrite,
    UpdateLockedWrite,
    UpdateReturningWrite,
    UpdateWrite,
    WriteStatement,
)
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Album(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="wst_albums")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    rank: int = ormar.Integer(default=0)


class Track(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="wst_tracks")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=20)
    position: Optional[int] = ormar.Integer(nullable=True)
    album: Optional[Album] = ormar.ForeignKey(Album, nullable=True)


class Release(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="wst_releases")

    region: str = ormar.String(max_length=5, primary_key=True)
    number: int = ormar.Integer(primary_key=True)
    copies: int = ormar.Integer(default=0)
    album: Optional[Album] = ormar.ForeignKey(Album, nullable=True)


create_test_database = init_tests(base_ormar_config)

KINDS: dict[str, type[WriteStatement]] = {
    "update": UpdateWrite,
    "delete": DeleteWrite,
    "update_returning": UpdateReturningWrite,
    "lock": LockWrite,
    "update_locked": UpdateLockedWrite,
}
DIALECTS = (sqlite.dialect(), postgresql.dialect(), mysql.dialect())

# name -> (queryset, updated values); the delete ignores the values
CASES: dict[str, Callable[[], tuple[QuerySet, dict]]] = {
    "all_rows": lambda: (Track.objects, {"position": 3}),
    "filters": lambda: (
        Track.objects.filter(title="a").exclude(position=1),
        {"position": 3},
    ),
    "filters_expression": lambda: (
        Track.objects.filter(title="a"),
        {"position": (F("album__rank") + 1)._node()},
    ),
    "related_filters": lambda: (
        Track.objects.filter(album__name="x").exclude(title="b"),
        {"position": 3},
    ),
    "related_filters_expression": lambda: (
        Track.objects.filter(album__name="x"),
        {"position": F("album__rank")._node()},
    ),
    "composite_key_related": lambda: (
        Release.objects.filter(album__name="x"),
        {"copies": 2},
    ),
}


# SQL of each case, statement and dialect as built before the statement classes
EXPECTED = {
    "all_rows": {
        "update": {
            "sqlite": "UPDATE wst_tracks SET position=?",
            "postgresql": "UPDATE wst_tracks SET position=%(ormar_v0)s",
            "mysql": "UPDATE wst_tracks SET position=%s",
        },
        "delete": {
            "sqlite": "DELETE FROM wst_tracks",
            "postgresql": "DELETE FROM wst_tracks",
            "mysql": "DELETE FROM wst_tracks",
        },
        "update_returning": {
            "sqlite": (
                "UPDATE wst_tracks SET position=? RETURNING id, title, position, album"
            ),
            "postgresql": (
                "UPDATE wst_tracks SET position=%(ormar_v0)s RETURNING "
                "wst_tracks.id, wst_tracks.title, wst_tracks.position, "
                "wst_tracks.album"
            ),
            "mysql": (
                "UPDATE wst_tracks SET position=%s RETURNING wst_tracks.id, "
                "wst_tracks.title, wst_tracks.position, wst_tracks.album"
            ),
        },
        "lock": {
            "sqlite": "SELECT wst_tracks.id FROM wst_tracks",
            "postgresql": (
                "SELECT wst_tracks.id FROM wst_tracks FOR UPDATE OF wst_tracks"
            ),
            "mysql": "SELECT wst_tracks.id FROM wst_tracks FOR UPDATE",
        },
        "update_locked": {
            "sqlite": (
                "UPDATE wst_tracks SET position=? WHERE wst_tracks.id IN "
                "(__[POSTCOMPILE_ormar_pks])"
            ),
            "postgresql": (
                "UPDATE wst_tracks SET position=%(ormar_v0)s WHERE "
                "wst_tracks.id IN (__[POSTCOMPILE_ormar_pks])"
            ),
            "mysql": (
                "UPDATE wst_tracks SET position=%s WHERE wst_tracks.id IN "
                "(__[POSTCOMPILE_ormar_pks])"
            ),
        },
    },
    "filters": {
        "update": {
            "sqlite": (
                "UPDATE wst_tracks SET position=? WHERE wst_tracks.title = ? "
                "AND NOT (wst_tracks.position = ?)"
            ),
            "postgresql": (
                "UPDATE wst_tracks SET position=%(ormar_v0)s WHERE "
                "wst_tracks.title = %(ormar_p0)s AND NOT (wst_tracks.position "
                "= %(ormar_p1)s)"
            ),
            "mysql": (
                "UPDATE wst_tracks SET position=%s WHERE wst_tracks.title = "
                "%s AND NOT (wst_tracks.position = %s)"
            ),
        },
        "delete": {
            "sqlite": (
                "DELETE FROM wst_tracks WHERE wst_tracks.title = ? AND NOT "
                "(wst_tracks.position = ?)"
            ),
            "postgresql": (
                "DELETE FROM wst_tracks WHERE wst_tracks.title = %(ormar_p0)s "
                "AND NOT (wst_tracks.position = %(ormar_p1)s)"
            ),
            "mysql": (
                "DELETE FROM wst_tracks WHERE wst_tracks.title = %s AND NOT "
                "(wst_tracks.position = %s)"
            ),
        },
        "update_returning": {
            "sqlite": (
                "UPDATE wst_tracks SET position=? WHERE wst_tracks.title = ? "
                "AND NOT (wst_tracks.position = ?) RETURNING id, title, "
                "position, album"
            ),
            "postgresql": (
                "UPDATE wst_tracks SET position=%(ormar_v0)s WHERE "
                "wst_tracks.title = %(ormar_p0)s AND NOT (wst_tracks.position "
                "= %(ormar_p1)s) RETURNING wst_tracks.id, wst_tracks.title, "
                "wst_tracks.position, wst_tracks.album"
            ),
            "mysql": (
                "UPDATE wst_tracks SET position=%s WHERE wst_tracks.title = "
                "%s AND NOT (wst_tracks.position = %s) RETURNING "
                "wst_tracks.id, wst_tracks.title, wst_tracks.position, "
                "wst_tracks.album"
            ),
        },
        "lock": {
            "sqlite": (
                "SELECT wst_tracks.id FROM wst_tracks WHERE wst_tracks.title "
                "= ? AND NOT (wst_tracks.position = ?)"
            ),
            "postgresql": (
                "SELECT wst_tracks.id FROM wst_tracks WHERE wst_tracks.title "
                "= %(ormar_p0)s AND NOT (wst_tracks.position = %(ormar_p1)s) "
                "FOR UPDATE OF wst_tracks"
            ),
            "mysql": (
                "SELECT wst_tracks.id FROM wst_tracks WHERE wst_tracks.title "
                "= %s AND NOT (wst_tracks.position = %s) FOR UPDATE"
            ),
        },
        "update_locked": {
            "sqlite": (
                "UPDATE wst_tracks SET position=? WHERE wst_tracks.id IN "
                "(__[POSTCOMPILE_ormar_pks])"
            ),
            "postgresql": (
                "UPDATE wst_tracks SET position=%(ormar_v0)s WHERE "
                "wst_tracks.id IN (__[POSTCOMPILE_ormar_pks])"
            ),
            "mysql": (
                "UPDATE wst_tracks SET position=%s WHERE wst_tracks.id IN "
                "(__[POSTCOMPILE_ormar_pks])"
            ),
        },
    },
    "filters_expression": {
        "update": {
            "sqlite": (
                "UPDATE wst_tracks SET position=(t1b26c7_wst_albums.rank + ?) "
                "FROM wst_albums AS t1b26c7_wst_albums WHERE "
                "t1b26c7_wst_albums.id = wst_tracks.album AND "
                "wst_tracks.title = ?"
            ),
            "postgresql": (
                "UPDATE wst_tracks SET position=(t1b26c7_wst_albums.rank + "
                "%(ormar_v0_0)s) FROM wst_albums AS t1b26c7_wst_albums WHERE "
                "t1b26c7_wst_albums.id = wst_tracks.album AND "
                "wst_tracks.title = %(ormar_p0)s"
            ),
            "mysql": (
                "UPDATE wst_tracks, wst_albums AS t1b26c7_wst_albums SET "
                "wst_tracks.position=(t1b26c7_wst_albums.`rank` + %s) WHERE "
                "t1b26c7_wst_albums.id = wst_tracks.album AND "
                "wst_tracks.title = %s"
            ),
        },
        "delete": {
            "sqlite": "DELETE FROM wst_tracks WHERE wst_tracks.title = ?",
            "postgresql": (
                "DELETE FROM wst_tracks WHERE wst_tracks.title = %(ormar_p0)s"
            ),
            "mysql": ("DELETE FROM wst_tracks WHERE wst_tracks.title = %s"),
        },
        "update_returning": {
            "sqlite": (
                "UPDATE wst_tracks SET position=(t1b26c7_wst_albums.rank + ?) "
                "FROM wst_albums AS t1b26c7_wst_albums WHERE "
                "t1b26c7_wst_albums.id = wst_tracks.album AND "
                "wst_tracks.title = ? RETURNING id, title, position, album"
            ),
            "postgresql": (
                "UPDATE wst_tracks SET position=(t1b26c7_wst_albums.rank + "
                "%(ormar_v0_0)s) FROM wst_albums AS t1b26c7_wst_albums WHERE "
                "t1b26c7_wst_albums.id = wst_tracks.album AND "
                "wst_tracks.title = %(ormar_p0)s RETURNING wst_tracks.id, "
                "wst_tracks.title, wst_tracks.position, wst_tracks.album"
            ),
            "mysql": (
                "UPDATE wst_tracks, wst_albums AS t1b26c7_wst_albums SET "
                "wst_tracks.position=(t1b26c7_wst_albums.`rank` + %s) WHERE "
                "t1b26c7_wst_albums.id = wst_tracks.album AND "
                "wst_tracks.title = %s RETURNING wst_tracks.id, "
                "wst_tracks.title, wst_tracks.position, wst_tracks.album"
            ),
        },
        "lock": {
            "sqlite": (
                "SELECT wst_tracks.id FROM wst_tracks, wst_albums AS "
                "t1b26c7_wst_albums WHERE t1b26c7_wst_albums.id = "
                "wst_tracks.album AND wst_tracks.title = ?"
            ),
            "postgresql": (
                "SELECT wst_tracks.id FROM wst_tracks, wst_albums AS "
                "t1b26c7_wst_albums WHERE t1b26c7_wst_albums.id = "
                "wst_tracks.album AND wst_tracks.title = %(ormar_p0)s FOR "
                "UPDATE OF wst_tracks"
            ),
            "mysql": (
                "SELECT wst_tracks.id FROM wst_tracks, wst_albums AS "
                "t1b26c7_wst_albums WHERE t1b26c7_wst_albums.id = "
                "wst_tracks.album AND wst_tracks.title = %s FOR UPDATE"
            ),
        },
        "update_locked": {
            "sqlite": (
                "UPDATE wst_tracks SET position=(t1b26c7_wst_albums.rank + ?) "
                "FROM wst_albums AS t1b26c7_wst_albums WHERE "
                "t1b26c7_wst_albums.id = wst_tracks.album AND wst_tracks.id "
                "IN (__[POSTCOMPILE_ormar_pks])"
            ),
            "postgresql": (
                "UPDATE wst_tracks SET position=(t1b26c7_wst_albums.rank + "
                "%(ormar_v0_0)s) FROM wst_albums AS t1b26c7_wst_albums WHERE "
                "t1b26c7_wst_albums.id = wst_tracks.album AND wst_tracks.id "
                "IN (__[POSTCOMPILE_ormar_pks])"
            ),
            "mysql": (
                "UPDATE wst_tracks, wst_albums AS t1b26c7_wst_albums SET "
                "wst_tracks.position=(t1b26c7_wst_albums.`rank` + %s) WHERE "
                "t1b26c7_wst_albums.id = wst_tracks.album AND wst_tracks.id "
                "IN (__[POSTCOMPILE_ormar_pks])"
            ),
        },
    },
    "related_filters": {
        "update": {
            "sqlite": (
                "UPDATE wst_tracks SET position=? WHERE wst_tracks.id IN "
                "(SELECT ormar_matched.id FROM (SELECT wst_tracks.id AS id "
                "FROM wst_tracks LEFT OUTER JOIN wst_albums AS "
                "t1b26c7_wst_albums ON t1b26c7_wst_albums.id=wst_tracks.album "
                "WHERE t1b26c7_wst_albums.name = ? AND NOT (wst_tracks.title "
                "= ?)) AS ormar_matched)"
            ),
            "postgresql": (
                "UPDATE wst_tracks SET position=%(ormar_v0)s WHERE "
                "wst_tracks.id IN (SELECT ormar_matched.id FROM (SELECT "
                "wst_tracks.id AS id FROM wst_tracks LEFT OUTER JOIN "
                "wst_albums AS t1b26c7_wst_albums ON "
                "t1b26c7_wst_albums.id=wst_tracks.album WHERE "
                "t1b26c7_wst_albums.name = %(ormar_p0)s AND NOT "
                "(wst_tracks.title = %(ormar_p1)s)) AS ormar_matched)"
            ),
            "mysql": (
                "UPDATE wst_tracks SET position=%s WHERE wst_tracks.id IN "
                "(SELECT ormar_matched.id FROM (SELECT wst_tracks.id AS id "
                "FROM wst_tracks LEFT OUTER JOIN wst_albums AS "
                "t1b26c7_wst_albums ON t1b26c7_wst_albums.id=wst_tracks.album "
                "WHERE t1b26c7_wst_albums.name = %s AND NOT (wst_tracks.title "
                "= %s)) AS ormar_matched)"
            ),
        },
        "delete": {
            "sqlite": (
                "DELETE FROM wst_tracks WHERE wst_tracks.id IN (SELECT "
                "ormar_matched.id FROM (SELECT wst_tracks.id AS id FROM "
                "wst_tracks LEFT OUTER JOIN wst_albums AS t1b26c7_wst_albums "
                "ON t1b26c7_wst_albums.id=wst_tracks.album WHERE "
                "t1b26c7_wst_albums.name = ? AND NOT (wst_tracks.title = ?)) "
                "AS ormar_matched)"
            ),
            "postgresql": (
                "DELETE FROM wst_tracks WHERE wst_tracks.id IN (SELECT "
                "ormar_matched.id FROM (SELECT wst_tracks.id AS id FROM "
                "wst_tracks LEFT OUTER JOIN wst_albums AS t1b26c7_wst_albums "
                "ON t1b26c7_wst_albums.id=wst_tracks.album WHERE "
                "t1b26c7_wst_albums.name = %(ormar_p0)s AND NOT "
                "(wst_tracks.title = %(ormar_p1)s)) AS ormar_matched)"
            ),
            "mysql": (
                "DELETE FROM wst_tracks WHERE wst_tracks.id IN (SELECT "
                "ormar_matched.id FROM (SELECT wst_tracks.id AS id FROM "
                "wst_tracks LEFT OUTER JOIN wst_albums AS t1b26c7_wst_albums "
                "ON t1b26c7_wst_albums.id=wst_tracks.album WHERE "
                "t1b26c7_wst_albums.name = %s AND NOT (wst_tracks.title = "
                "%s)) AS ormar_matched)"
            ),
        },
        "update_returning": {
            "sqlite": (
                "UPDATE wst_tracks SET position=? WHERE wst_tracks.id IN "
                "(SELECT ormar_matched.id FROM (SELECT wst_tracks.id AS id "
                "FROM wst_tracks LEFT OUTER JOIN wst_albums AS "
                "t1b26c7_wst_albums ON t1b26c7_wst_albums.id=wst_tracks.album "
                "WHERE t1b26c7_wst_albums.name = ? AND NOT (wst_tracks.title "
                "= ?)) AS ormar_matched) RETURNING id, title, position, album"
            ),
            "postgresql": (
                "UPDATE wst_tracks SET position=%(ormar_v0)s WHERE "
                "wst_tracks.id IN (SELECT ormar_matched.id FROM (SELECT "
                "wst_tracks.id AS id FROM wst_tracks LEFT OUTER JOIN "
                "wst_albums AS t1b26c7_wst_albums ON "
                "t1b26c7_wst_albums.id=wst_tracks.album WHERE "
                "t1b26c7_wst_albums.name = %(ormar_p0)s AND NOT "
                "(wst_tracks.title = %(ormar_p1)s)) AS ormar_matched) "
                "RETURNING wst_tracks.id, wst_tracks.title, "
                "wst_tracks.position, wst_tracks.album"
            ),
            "mysql": (
                "UPDATE wst_tracks SET position=%s WHERE wst_tracks.id IN "
                "(SELECT ormar_matched.id FROM (SELECT wst_tracks.id AS id "
                "FROM wst_tracks LEFT OUTER JOIN wst_albums AS "
                "t1b26c7_wst_albums ON t1b26c7_wst_albums.id=wst_tracks.album "
                "WHERE t1b26c7_wst_albums.name = %s AND NOT (wst_tracks.title "
                "= %s)) AS ormar_matched) RETURNING wst_tracks.id, "
                "wst_tracks.title, wst_tracks.position, wst_tracks.album"
            ),
        },
        "lock": {
            "sqlite": (
                "SELECT wst_tracks.id FROM wst_tracks WHERE wst_tracks.id IN "
                "(SELECT ormar_matched.id FROM (SELECT wst_tracks.id AS id "
                "FROM wst_tracks LEFT OUTER JOIN wst_albums AS "
                "t1b26c7_wst_albums ON t1b26c7_wst_albums.id=wst_tracks.album "
                "WHERE t1b26c7_wst_albums.name = ? AND NOT (wst_tracks.title "
                "= ?)) AS ormar_matched)"
            ),
            "postgresql": (
                "SELECT wst_tracks.id FROM wst_tracks WHERE wst_tracks.id IN "
                "(SELECT ormar_matched.id FROM (SELECT wst_tracks.id AS id "
                "FROM wst_tracks LEFT OUTER JOIN wst_albums AS "
                "t1b26c7_wst_albums ON t1b26c7_wst_albums.id=wst_tracks.album "
                "WHERE t1b26c7_wst_albums.name = %(ormar_p0)s AND NOT "
                "(wst_tracks.title = %(ormar_p1)s)) AS ormar_matched) FOR "
                "UPDATE OF wst_tracks"
            ),
            "mysql": (
                "SELECT wst_tracks.id FROM wst_tracks WHERE wst_tracks.id IN "
                "(SELECT ormar_matched.id FROM (SELECT wst_tracks.id AS id "
                "FROM wst_tracks LEFT OUTER JOIN wst_albums AS "
                "t1b26c7_wst_albums ON t1b26c7_wst_albums.id=wst_tracks.album "
                "WHERE t1b26c7_wst_albums.name = %s AND NOT (wst_tracks.title "
                "= %s)) AS ormar_matched) FOR UPDATE"
            ),
        },
        "update_locked": {
            "sqlite": (
                "UPDATE wst_tracks SET position=? WHERE wst_tracks.id IN "
                "(__[POSTCOMPILE_ormar_pks])"
            ),
            "postgresql": (
                "UPDATE wst_tracks SET position=%(ormar_v0)s WHERE "
                "wst_tracks.id IN (__[POSTCOMPILE_ormar_pks])"
            ),
            "mysql": (
                "UPDATE wst_tracks SET position=%s WHERE wst_tracks.id IN "
                "(__[POSTCOMPILE_ormar_pks])"
            ),
        },
    },
    "related_filters_expression": {
        "update": {
            "sqlite": (
                "UPDATE wst_tracks SET position=t1b26c7_wst_albums.rank FROM "
                "wst_albums AS t1b26c7_wst_albums WHERE t1b26c7_wst_albums.id "
                "= wst_tracks.album AND wst_tracks.id IN (SELECT "
                "ormar_matched.id FROM (SELECT wst_tracks.id AS id FROM "
                "wst_tracks LEFT OUTER JOIN wst_albums AS t1b26c7_wst_albums "
                "ON t1b26c7_wst_albums.id=wst_tracks.album WHERE "
                "t1b26c7_wst_albums.name = ?) AS ormar_matched)"
            ),
            "postgresql": (
                "UPDATE wst_tracks SET position=t1b26c7_wst_albums.rank FROM "
                "wst_albums AS t1b26c7_wst_albums WHERE t1b26c7_wst_albums.id "
                "= wst_tracks.album AND wst_tracks.id IN (SELECT "
                "ormar_matched.id FROM (SELECT wst_tracks.id AS id FROM "
                "wst_tracks LEFT OUTER JOIN wst_albums AS t1b26c7_wst_albums "
                "ON t1b26c7_wst_albums.id=wst_tracks.album WHERE "
                "t1b26c7_wst_albums.name = %(ormar_p0)s) AS ormar_matched)"
            ),
            "mysql": (
                "UPDATE wst_tracks, wst_albums AS t1b26c7_wst_albums SET "
                "wst_tracks.position=t1b26c7_wst_albums.`rank` WHERE "
                "t1b26c7_wst_albums.id = wst_tracks.album AND wst_tracks.id "
                "IN (SELECT ormar_matched.id FROM (SELECT wst_tracks.id AS id "
                "FROM wst_tracks LEFT OUTER JOIN wst_albums AS "
                "t1b26c7_wst_albums ON t1b26c7_wst_albums.id=wst_tracks.album "
                "WHERE t1b26c7_wst_albums.name = %s) AS ormar_matched)"
            ),
        },
        "delete": {
            "sqlite": (
                "DELETE FROM wst_tracks WHERE wst_tracks.id IN (SELECT "
                "ormar_matched.id FROM (SELECT wst_tracks.id AS id FROM "
                "wst_tracks LEFT OUTER JOIN wst_albums AS t1b26c7_wst_albums "
                "ON t1b26c7_wst_albums.id=wst_tracks.album WHERE "
                "t1b26c7_wst_albums.name = ?) AS ormar_matched)"
            ),
            "postgresql": (
                "DELETE FROM wst_tracks WHERE wst_tracks.id IN (SELECT "
                "ormar_matched.id FROM (SELECT wst_tracks.id AS id FROM "
                "wst_tracks LEFT OUTER JOIN wst_albums AS t1b26c7_wst_albums "
                "ON t1b26c7_wst_albums.id=wst_tracks.album WHERE "
                "t1b26c7_wst_albums.name = %(ormar_p0)s) AS ormar_matched)"
            ),
            "mysql": (
                "DELETE FROM wst_tracks WHERE wst_tracks.id IN (SELECT "
                "ormar_matched.id FROM (SELECT wst_tracks.id AS id FROM "
                "wst_tracks LEFT OUTER JOIN wst_albums AS t1b26c7_wst_albums "
                "ON t1b26c7_wst_albums.id=wst_tracks.album WHERE "
                "t1b26c7_wst_albums.name = %s) AS ormar_matched)"
            ),
        },
        "update_returning": {
            "sqlite": (
                "UPDATE wst_tracks SET position=t1b26c7_wst_albums.rank FROM "
                "wst_albums AS t1b26c7_wst_albums WHERE t1b26c7_wst_albums.id "
                "= wst_tracks.album AND wst_tracks.id IN (SELECT "
                "ormar_matched.id FROM (SELECT wst_tracks.id AS id FROM "
                "wst_tracks LEFT OUTER JOIN wst_albums AS t1b26c7_wst_albums "
                "ON t1b26c7_wst_albums.id=wst_tracks.album WHERE "
                "t1b26c7_wst_albums.name = ?) AS ormar_matched) RETURNING id, "
                "title, position, album"
            ),
            "postgresql": (
                "UPDATE wst_tracks SET position=t1b26c7_wst_albums.rank FROM "
                "wst_albums AS t1b26c7_wst_albums WHERE t1b26c7_wst_albums.id "
                "= wst_tracks.album AND wst_tracks.id IN (SELECT "
                "ormar_matched.id FROM (SELECT wst_tracks.id AS id FROM "
                "wst_tracks LEFT OUTER JOIN wst_albums AS t1b26c7_wst_albums "
                "ON t1b26c7_wst_albums.id=wst_tracks.album WHERE "
                "t1b26c7_wst_albums.name = %(ormar_p0)s) AS ormar_matched) "
                "RETURNING wst_tracks.id, wst_tracks.title, "
                "wst_tracks.position, wst_tracks.album"
            ),
            "mysql": (
                "UPDATE wst_tracks, wst_albums AS t1b26c7_wst_albums SET "
                "wst_tracks.position=t1b26c7_wst_albums.`rank` WHERE "
                "t1b26c7_wst_albums.id = wst_tracks.album AND wst_tracks.id "
                "IN (SELECT ormar_matched.id FROM (SELECT wst_tracks.id AS id "
                "FROM wst_tracks LEFT OUTER JOIN wst_albums AS "
                "t1b26c7_wst_albums ON t1b26c7_wst_albums.id=wst_tracks.album "
                "WHERE t1b26c7_wst_albums.name = %s) AS ormar_matched) "
                "RETURNING wst_tracks.id, wst_tracks.title, "
                "wst_tracks.position, wst_tracks.album"
            ),
        },
        "lock": {
            "sqlite": (
                "SELECT wst_tracks.id FROM wst_tracks, wst_albums AS "
                "t1b26c7_wst_albums WHERE t1b26c7_wst_albums.id = "
                "wst_tracks.album AND wst_tracks.id IN (SELECT "
                "ormar_matched.id FROM (SELECT wst_tracks.id AS id FROM "
                "wst_tracks LEFT OUTER JOIN wst_albums AS t1b26c7_wst_albums "
                "ON t1b26c7_wst_albums.id=wst_tracks.album WHERE "
                "t1b26c7_wst_albums.name = ?) AS ormar_matched)"
            ),
            "postgresql": (
                "SELECT wst_tracks.id FROM wst_tracks, wst_albums AS "
                "t1b26c7_wst_albums WHERE t1b26c7_wst_albums.id = "
                "wst_tracks.album AND wst_tracks.id IN (SELECT "
                "ormar_matched.id FROM (SELECT wst_tracks.id AS id FROM "
                "wst_tracks LEFT OUTER JOIN wst_albums AS t1b26c7_wst_albums "
                "ON t1b26c7_wst_albums.id=wst_tracks.album WHERE "
                "t1b26c7_wst_albums.name = %(ormar_p0)s) AS ormar_matched) "
                "FOR UPDATE OF wst_tracks"
            ),
            "mysql": (
                "SELECT wst_tracks.id FROM wst_tracks, wst_albums AS "
                "t1b26c7_wst_albums WHERE t1b26c7_wst_albums.id = "
                "wst_tracks.album AND wst_tracks.id IN (SELECT "
                "ormar_matched.id FROM (SELECT wst_tracks.id AS id FROM "
                "wst_tracks LEFT OUTER JOIN wst_albums AS t1b26c7_wst_albums "
                "ON t1b26c7_wst_albums.id=wst_tracks.album WHERE "
                "t1b26c7_wst_albums.name = %s) AS ormar_matched) FOR UPDATE"
            ),
        },
        "update_locked": {
            "sqlite": (
                "UPDATE wst_tracks SET position=t1b26c7_wst_albums.rank FROM "
                "wst_albums AS t1b26c7_wst_albums WHERE t1b26c7_wst_albums.id "
                "= wst_tracks.album AND wst_tracks.id IN "
                "(__[POSTCOMPILE_ormar_pks])"
            ),
            "postgresql": (
                "UPDATE wst_tracks SET position=t1b26c7_wst_albums.rank FROM "
                "wst_albums AS t1b26c7_wst_albums WHERE t1b26c7_wst_albums.id "
                "= wst_tracks.album AND wst_tracks.id IN "
                "(__[POSTCOMPILE_ormar_pks])"
            ),
            "mysql": (
                "UPDATE wst_tracks, wst_albums AS t1b26c7_wst_albums SET "
                "wst_tracks.position=t1b26c7_wst_albums.`rank` WHERE "
                "t1b26c7_wst_albums.id = wst_tracks.album AND wst_tracks.id "
                "IN (__[POSTCOMPILE_ormar_pks])"
            ),
        },
    },
    "composite_key_related": {
        "update": {
            "sqlite": (
                "UPDATE wst_releases SET copies=? WHERE (wst_releases.region, "
                "wst_releases.number) IN (SELECT ormar_matched.region, "
                "ormar_matched.number FROM (SELECT wst_releases.region AS "
                "region, wst_releases.number AS number FROM wst_releases LEFT "
                "OUTER JOIN wst_albums AS t1b26c7_wst_albums ON "
                "t1b26c7_wst_albums.id=wst_releases.album WHERE "
                "t1b26c7_wst_albums.name = ?) AS ormar_matched)"
            ),
            "postgresql": (
                "UPDATE wst_releases SET copies=%(ormar_v0)s WHERE "
                "(wst_releases.region, wst_releases.number) IN (SELECT "
                "ormar_matched.region, ormar_matched.number FROM (SELECT "
                "wst_releases.region AS region, wst_releases.number AS number "
                "FROM wst_releases LEFT OUTER JOIN wst_albums AS "
                "t1b26c7_wst_albums ON "
                "t1b26c7_wst_albums.id=wst_releases.album WHERE "
                "t1b26c7_wst_albums.name = %(ormar_p0)s) AS ormar_matched)"
            ),
            "mysql": (
                "UPDATE wst_releases SET copies=%s WHERE "
                "(wst_releases.region, wst_releases.number) IN (SELECT "
                "ormar_matched.region, ormar_matched.number FROM (SELECT "
                "wst_releases.region AS region, wst_releases.number AS number "
                "FROM wst_releases LEFT OUTER JOIN wst_albums AS "
                "t1b26c7_wst_albums ON "
                "t1b26c7_wst_albums.id=wst_releases.album WHERE "
                "t1b26c7_wst_albums.name = %s) AS ormar_matched)"
            ),
        },
        "delete": {
            "sqlite": (
                "DELETE FROM wst_releases WHERE (wst_releases.region, "
                "wst_releases.number) IN (SELECT ormar_matched.region, "
                "ormar_matched.number FROM (SELECT wst_releases.region AS "
                "region, wst_releases.number AS number FROM wst_releases LEFT "
                "OUTER JOIN wst_albums AS t1b26c7_wst_albums ON "
                "t1b26c7_wst_albums.id=wst_releases.album WHERE "
                "t1b26c7_wst_albums.name = ?) AS ormar_matched)"
            ),
            "postgresql": (
                "DELETE FROM wst_releases WHERE (wst_releases.region, "
                "wst_releases.number) IN (SELECT ormar_matched.region, "
                "ormar_matched.number FROM (SELECT wst_releases.region AS "
                "region, wst_releases.number AS number FROM wst_releases LEFT "
                "OUTER JOIN wst_albums AS t1b26c7_wst_albums ON "
                "t1b26c7_wst_albums.id=wst_releases.album WHERE "
                "t1b26c7_wst_albums.name = %(ormar_p0)s) AS ormar_matched)"
            ),
            "mysql": (
                "DELETE FROM wst_releases WHERE (wst_releases.region, "
                "wst_releases.number) IN (SELECT ormar_matched.region, "
                "ormar_matched.number FROM (SELECT wst_releases.region AS "
                "region, wst_releases.number AS number FROM wst_releases LEFT "
                "OUTER JOIN wst_albums AS t1b26c7_wst_albums ON "
                "t1b26c7_wst_albums.id=wst_releases.album WHERE "
                "t1b26c7_wst_albums.name = %s) AS ormar_matched)"
            ),
        },
        "update_returning": {
            "sqlite": (
                "UPDATE wst_releases SET copies=? WHERE (wst_releases.region, "
                "wst_releases.number) IN (SELECT ormar_matched.region, "
                "ormar_matched.number FROM (SELECT wst_releases.region AS "
                "region, wst_releases.number AS number FROM wst_releases LEFT "
                "OUTER JOIN wst_albums AS t1b26c7_wst_albums ON "
                "t1b26c7_wst_albums.id=wst_releases.album WHERE "
                "t1b26c7_wst_albums.name = ?) AS ormar_matched) RETURNING "
                "region, number, copies, album"
            ),
            "postgresql": (
                "UPDATE wst_releases SET copies=%(ormar_v0)s WHERE "
                "(wst_releases.region, wst_releases.number) IN (SELECT "
                "ormar_matched.region, ormar_matched.number FROM (SELECT "
                "wst_releases.region AS region, wst_releases.number AS number "
                "FROM wst_releases LEFT OUTER JOIN wst_albums AS "
                "t1b26c7_wst_albums ON "
                "t1b26c7_wst_albums.id=wst_releases.album WHERE "
                "t1b26c7_wst_albums.name = %(ormar_p0)s) AS ormar_matched) "
                "RETURNING wst_releases.region, wst_releases.number, "
                "wst_releases.copies, wst_releases.album"
            ),
            "mysql": (
                "UPDATE wst_releases SET copies=%s WHERE "
                "(wst_releases.region, wst_releases.number) IN (SELECT "
                "ormar_matched.region, ormar_matched.number FROM (SELECT "
                "wst_releases.region AS region, wst_releases.number AS number "
                "FROM wst_releases LEFT OUTER JOIN wst_albums AS "
                "t1b26c7_wst_albums ON "
                "t1b26c7_wst_albums.id=wst_releases.album WHERE "
                "t1b26c7_wst_albums.name = %s) AS ormar_matched) RETURNING "
                "wst_releases.region, wst_releases.number, "
                "wst_releases.copies, wst_releases.album"
            ),
        },
        "lock": {
            "sqlite": (
                "SELECT wst_releases.region, wst_releases.number FROM "
                "wst_releases WHERE (wst_releases.region, "
                "wst_releases.number) IN (SELECT ormar_matched.region, "
                "ormar_matched.number FROM (SELECT wst_releases.region AS "
                "region, wst_releases.number AS number FROM wst_releases LEFT "
                "OUTER JOIN wst_albums AS t1b26c7_wst_albums ON "
                "t1b26c7_wst_albums.id=wst_releases.album WHERE "
                "t1b26c7_wst_albums.name = ?) AS ormar_matched)"
            ),
            "postgresql": (
                "SELECT wst_releases.region, wst_releases.number FROM "
                "wst_releases WHERE (wst_releases.region, "
                "wst_releases.number) IN (SELECT ormar_matched.region, "
                "ormar_matched.number FROM (SELECT wst_releases.region AS "
                "region, wst_releases.number AS number FROM wst_releases LEFT "
                "OUTER JOIN wst_albums AS t1b26c7_wst_albums ON "
                "t1b26c7_wst_albums.id=wst_releases.album WHERE "
                "t1b26c7_wst_albums.name = %(ormar_p0)s) AS ormar_matched) "
                "FOR UPDATE OF wst_releases"
            ),
            "mysql": (
                "SELECT wst_releases.region, wst_releases.number FROM "
                "wst_releases WHERE (wst_releases.region, "
                "wst_releases.number) IN (SELECT ormar_matched.region, "
                "ormar_matched.number FROM (SELECT wst_releases.region AS "
                "region, wst_releases.number AS number FROM wst_releases LEFT "
                "OUTER JOIN wst_albums AS t1b26c7_wst_albums ON "
                "t1b26c7_wst_albums.id=wst_releases.album WHERE "
                "t1b26c7_wst_albums.name = %s) AS ormar_matched) FOR UPDATE"
            ),
        },
        "update_locked": {
            "sqlite": (
                "UPDATE wst_releases SET copies=? WHERE (wst_releases.region, "
                "wst_releases.number) IN (__[POSTCOMPILE_ormar_pks])"
            ),
            "postgresql": (
                "UPDATE wst_releases SET copies=%(ormar_v0)s WHERE "
                "(wst_releases.region, wst_releases.number) IN "
                "(__[POSTCOMPILE_ormar_pks])"
            ),
            "mysql": (
                "UPDATE wst_releases SET copies=%s WHERE "
                "(wst_releases.region, wst_releases.number) IN "
                "(__[POSTCOMPILE_ormar_pks])"
            ),
        },
    },
}


def compiled(statement: Any, dialect: Any) -> str:
    sql = " ".join(str(statement.compile(dialect=dialect)).split())
    # SQLAlchemy 2.1 casts postgres bind parameters (``::INTEGER``), 2.0 does not
    return re.sub(r"::[A-Z]+", "", sql)


def all_rendered() -> dict:
    result: dict = {}
    for case, factory in CASES.items():
        queryset, values = factory()
        for kind, statement in KINDS.items():
            template = queryset.build_write_template(
                statement=statement, values={} if statement is DeleteWrite else values
            )
            for dialect in DIALECTS:
                result.setdefault(case, {}).setdefault(kind, {})[dialect.name] = (
                    compiled(statement=template.statement, dialect=dialect)
                )
    return result


@pytest.mark.asyncio
async def test_write_statements_are_unchanged():
    """
    Every filtered statement class for each way of restricting the
    rows (all rows, filters, matched rows of related filters, composite key)
    with plain values and expressions reading related models, on each dialect.
    """
    async with base_ormar_config.database:
        assert all_rendered() == EXPECTED


# SQL of the statements by primary key of each model as built before the
# statement classes: single-row writes, the locked read back of
# update_returning() and the per-row update of bulk_update (by the current
# key, or by the persisted one when a key moved)
EXPECTED_ROWS = {
    "Track": {
        "update": {
            "sqlite": "UPDATE wst_tracks SET title=?, position=? "
            "WHERE wst_tracks.id = ?",
            "postgresql": "UPDATE wst_tracks SET title=%(ormar_v1)s, "
            "position=%(ormar_v0)s WHERE wst_tracks.id "
            "= %(ormar_pk)s",
            "mysql": "UPDATE wst_tracks SET title=%s, position=%s "
            "WHERE wst_tracks.id = %s",
        },
        "delete": {
            "sqlite": "DELETE FROM wst_tracks WHERE wst_tracks.id = ?",
            "postgresql": "DELETE FROM wst_tracks WHERE wst_tracks.id = %(ormar_pk)s",
            "mysql": "DELETE FROM wst_tracks WHERE wst_tracks.id = %s",
        },
        "select_pk": {
            "sqlite": "SELECT wst_tracks.id, wst_tracks.title, "
            "wst_tracks.position, wst_tracks.album FROM "
            "wst_tracks WHERE wst_tracks.id = ?",
            "postgresql": "SELECT wst_tracks.id, "
            "wst_tracks.title, wst_tracks.position, "
            "wst_tracks.album FROM wst_tracks WHERE "
            "wst_tracks.id = %(ormar_pk)s",
            "mysql": "SELECT wst_tracks.id, wst_tracks.title, "
            "wst_tracks.position, wst_tracks.album FROM "
            "wst_tracks WHERE wst_tracks.id = %s",
        },
        "select_pks": {
            "sqlite": "SELECT wst_tracks.id, wst_tracks.title, "
            "wst_tracks.position, wst_tracks.album "
            "FROM wst_tracks WHERE wst_tracks.id IN "
            "(__[POSTCOMPILE_ormar_pks])",
            "postgresql": "SELECT wst_tracks.id, "
            "wst_tracks.title, "
            "wst_tracks.position, wst_tracks.album "
            "FROM wst_tracks WHERE wst_tracks.id "
            "IN (__[POSTCOMPILE_ormar_pks]) FOR "
            "UPDATE",
            "mysql": "SELECT wst_tracks.id, wst_tracks.title, "
            "wst_tracks.position, wst_tracks.album FROM "
            "wst_tracks WHERE wst_tracks.id IN "
            "(__[POSTCOMPILE_ormar_pks]) FOR UPDATE",
        },
        "bulk_new": {
            "sqlite": "UPDATE wst_tracks SET title=?, position=? "
            "WHERE wst_tracks.id = ?",
            "postgresql": "UPDATE wst_tracks SET "
            "title=%(new_title)s, "
            "position=%(new_position)s WHERE "
            "wst_tracks.id = %(new_id)s",
            "mysql": "UPDATE wst_tracks SET title=%s, position=%s "
            "WHERE wst_tracks.id = %s",
        },
        "bulk_old": {
            "sqlite": "UPDATE wst_tracks SET id=?, title=?, "
            "position=? WHERE wst_tracks.id = ?",
            "postgresql": "UPDATE wst_tracks SET id=%(new_id)s, "
            "title=%(new_title)s, "
            "position=%(new_position)s WHERE "
            "wst_tracks.id = %(old_id)s",
            "mysql": "UPDATE wst_tracks SET id=%s, title=%s, "
            "position=%s WHERE wst_tracks.id = %s",
        },
    },
    "Release": {
        "update": {
            "sqlite": "UPDATE wst_releases SET copies=? WHERE "
            "wst_releases.region = ? AND "
            "wst_releases.number = ?",
            "postgresql": "UPDATE wst_releases SET "
            "copies=%(ormar_v0)s WHERE "
            "wst_releases.region = %(ormar_pk_0)s "
            "AND wst_releases.number = "
            "%(ormar_pk_1)s",
            "mysql": "UPDATE wst_releases SET copies=%s WHERE "
            "wst_releases.region = %s AND "
            "wst_releases.number = %s",
        },
        "delete": {
            "sqlite": "DELETE FROM wst_releases WHERE "
            "wst_releases.region = ? AND "
            "wst_releases.number = ?",
            "postgresql": "DELETE FROM wst_releases WHERE "
            "wst_releases.region = %(ormar_pk_0)s "
            "AND wst_releases.number = "
            "%(ormar_pk_1)s",
            "mysql": "DELETE FROM wst_releases WHERE "
            "wst_releases.region = %s AND "
            "wst_releases.number = %s",
        },
        "select_pk": {
            "sqlite": "SELECT wst_releases.region, "
            "wst_releases.number, "
            "wst_releases.copies, wst_releases.album "
            "FROM wst_releases WHERE "
            "wst_releases.region = ? AND "
            "wst_releases.number = ?",
            "postgresql": "SELECT wst_releases.region, "
            "wst_releases.number, "
            "wst_releases.copies, "
            "wst_releases.album FROM wst_releases "
            "WHERE wst_releases.region = "
            "%(ormar_pk_0)s AND "
            "wst_releases.number = %(ormar_pk_1)s",
            "mysql": "SELECT wst_releases.region, "
            "wst_releases.number, wst_releases.copies, "
            "wst_releases.album FROM wst_releases "
            "WHERE wst_releases.region = %s AND "
            "wst_releases.number = %s",
        },
        "select_pks": {
            "sqlite": "SELECT wst_releases.region, "
            "wst_releases.number, "
            "wst_releases.copies, wst_releases.album "
            "FROM wst_releases WHERE "
            "(wst_releases.region, "
            "wst_releases.number) IN "
            "(__[POSTCOMPILE_ormar_pks])",
            "postgresql": "SELECT wst_releases.region, "
            "wst_releases.number, "
            "wst_releases.copies, "
            "wst_releases.album FROM "
            "wst_releases WHERE "
            "(wst_releases.region, "
            "wst_releases.number) IN "
            "(__[POSTCOMPILE_ormar_pks]) FOR "
            "UPDATE",
            "mysql": "SELECT wst_releases.region, "
            "wst_releases.number, "
            "wst_releases.copies, wst_releases.album "
            "FROM wst_releases WHERE "
            "(wst_releases.region, "
            "wst_releases.number) IN "
            "(__[POSTCOMPILE_ormar_pks]) FOR UPDATE",
        },
        "bulk_new": {
            "sqlite": "UPDATE wst_releases SET copies=? WHERE "
            "wst_releases.region = ? AND "
            "wst_releases.number = ?",
            "postgresql": "UPDATE wst_releases SET "
            "copies=%(new_copies)s WHERE "
            "wst_releases.region = %(new_region)s "
            "AND wst_releases.number = "
            "%(new_number)s",
            "mysql": "UPDATE wst_releases SET copies=%s WHERE "
            "wst_releases.region = %s AND "
            "wst_releases.number = %s",
        },
        "bulk_old": {
            "sqlite": "UPDATE wst_releases SET region=?, "
            "number=?, copies=? WHERE "
            "wst_releases.region = ? AND "
            "wst_releases.number = ?",
            "postgresql": "UPDATE wst_releases SET "
            "region=%(new_region)s, "
            "number=%(new_number)s, "
            "copies=%(new_copies)s WHERE "
            "wst_releases.region = %(old_region)s "
            "AND wst_releases.number = "
            "%(old_number)s",
            "mysql": "UPDATE wst_releases SET region=%s, "
            "number=%s, copies=%s WHERE "
            "wst_releases.region = %s AND "
            "wst_releases.number = %s",
        },
    },
}


def row_statement(model: Any, kind: str) -> Any:
    updated = ("position", "title") if model is Track else ("copies",)
    key_columns = model.ormar_config.pk.column_aliases()
    statements: dict[str, Callable[[], Any]] = {
        "update": lambda: UpdateWrite.make(
            model=model, values=dict.fromkeys(updated, 1)
        ),
        "delete": lambda: DeleteWrite.make(model=model),
        "select_pk": lambda: RowSelectWrite.make(model=model),
        "select_pks": lambda: RowsLockWrite.make(model=model),
        "bulk_new": lambda: BulkUpdateWrite.make(
            model=model, columns=updated, where_prefix="new_"
        ),
        "bulk_old": lambda: BulkUpdateWrite.make(
            model=model, columns=updated + key_columns, where_prefix="old_"
        ),
    }
    return statements[kind]()


def test_row_statements_are_unchanged():
    """
    Every statement by primary key for a single and a composite key, on each
    dialect.
    """
    result: dict = {}
    for model in (Track, Release):
        for kind in EXPECTED_ROWS[model.__name__]:
            for dialect in DIALECTS:
                result.setdefault(model.__name__, {}).setdefault(kind, {})[
                    dialect.name
                ] = compiled(statement=row_statement(model, kind), dialect=dialect)
    assert result == EXPECTED_ROWS
