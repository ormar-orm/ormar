import re
from typing import Any, Callable, Optional

import pytest
from sqlalchemy.dialects import mysql, postgresql, sqlite

import ormar
from ormar import F
from ormar.queryset.queryset import QuerySet
from ormar.queryset.templates import WriteKind
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

KINDS = tuple(WriteKind)
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


# SQL of each case, kind and dialect as built before WriteKind
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


def rendered(queryset: QuerySet, kind: WriteKind, values: dict, dialect: Any) -> str:
    template = queryset.build_write_template(
        kind=kind, values={} if kind is WriteKind.DELETE else values
    )
    statement: Any = template.statement
    sql = " ".join(str(statement.compile(dialect=dialect)).split())
    # SQLAlchemy 2.1 casts postgres bind parameters (``::INTEGER``), 2.0 does not
    return re.sub(r"::[A-Z]+", "", sql)


def all_rendered() -> dict:
    result: dict = {}
    for case, factory in CASES.items():
        queryset, values = factory()
        for kind in KINDS:
            for dialect in DIALECTS:
                result.setdefault(case, {}).setdefault(kind.value, {})[dialect.name] = (
                    rendered(
                        queryset=queryset, kind=kind, values=values, dialect=dialect
                    )
                )
    return result


@pytest.mark.asyncio
async def test_write_statements_are_unchanged():
    """
    Every kind of build_write_statement for each way of restricting the
    rows (all rows, filters, matched rows of related filters, composite key)
    with plain values and expressions reading related models, on each dialect.
    """
    async with base_ormar_config.database:
        assert all_rendered() == EXPECTED
