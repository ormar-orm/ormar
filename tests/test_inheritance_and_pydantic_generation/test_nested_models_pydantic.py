import ormar
from tests.lifespan import init_tests
from tests.schema_helpers import relation_schema
from tests.settings import create_config

base_ormar_config = create_config()


class Library(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Package(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    library: Library = ormar.ForeignKey(Library, related_name="packages")
    version: str = ormar.String(max_length=100)


class Ticket(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    number: int = ormar.Integer()
    status: str = ormar.String(max_length=100)


class TicketPackage(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    status: str = ormar.String(max_length=100)
    ticket: Ticket = ormar.ForeignKey(Ticket, related_name="packages")
    package: Package = ormar.ForeignKey(Package, related_name="tickets")


create_test_database = init_tests(base_ormar_config)


def test_have_proper_children():
    TicketPackageOut = TicketPackage.get_pydantic(exclude={"ticket"})
    assert "package" in TicketPackageOut.model_fields
    PydanticPackage = relation_schema(schema=TicketPackageOut, field="package")
    assert "library" in PydanticPackage.model_fields


def test_casts_properly():
    payload = {
        "id": 0,
        "status": "string",
        "ticket": {"id": 0, "number": 0, "status": "string"},
        "package": {
            "version": "string",
            "id": 0,
            "library": {"id": 0, "name": "string"},
        },
    }
    test_package = TicketPackage(**payload)
    TicketPackageOut = TicketPackage.get_pydantic(exclude={"ticket"})
    parsed = TicketPackageOut(**test_package.model_dump()).model_dump()
    assert "ticket" not in parsed
    assert "package" in parsed
    assert "library" in parsed.get("package")
