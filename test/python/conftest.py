# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2024 by the Nominatim developer community.
# For a full list of authors see the git log.
import itertools
import sys
from pathlib import Path

import dummy_tokenizer
import mocks
import psycopg
import pytest
from cursor import CursorForTesting
from psycopg import sql as pysql

import nominatim_db.tokenizer.factory
from nominatim_db.config import Configuration
from nominatim_db.db import connection
from nominatim_db.db.sql_preprocessor import SQLPreprocessor

# always test against the source
SRC_DIR = (Path(__file__) / ".." / ".." / "..").resolve()
sys.path.insert(0, str(SRC_DIR / "src"))


@pytest.fixture
def src_dir():
    return SRC_DIR


@pytest.fixture
def temp_db(monkeypatch):
    """Create an empty database for the test. The database name is also
    exported into NOMINATIM_DATABASE_DSN.
    """
    name = "test_nominatim_python_unittest"

    with psycopg.connect(dbname="postgres", autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(pysql.SQL("DROP DATABASE IF EXISTS") + pysql.Identifier(name))
            cur.execute(pysql.SQL("CREATE DATABASE") + pysql.Identifier(name))

    monkeypatch.setenv("NOMINATIM_DATABASE_DSN", "dbname=" + name)

    with psycopg.connect(dbname=name) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION hstore")

    yield name

    with psycopg.connect(dbname="postgres", autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP DATABASE IF EXISTS {}".format(name))


@pytest.fixture
def dsn(temp_db):
    return "dbname=" + temp_db


@pytest.fixture
def temp_db_with_extensions(temp_db):
    with psycopg.connect(dbname=temp_db) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION postgis")

    return temp_db


@pytest.fixture
def temp_db_conn(temp_db):
    """Connection to the test database."""
    with connection.connect("", autocommit=True, dbname=temp_db) as conn:
        connection.register_hstore(conn)
        yield conn


@pytest.fixture
def temp_db_cursor(temp_db):
    """Connection and cursor towards the test database. The connection will
    be in auto-commit mode.
    """
    with psycopg.connect(
        dbname=temp_db, autocommit=True, cursor_factory=CursorForTesting
    ) as conn:
        connection.register_hstore(conn)
        with conn.cursor() as cur:
            yield cur


@pytest.fixture
def table_factory(temp_db_conn):
    """A fixture that creates new SQL tables, potentially filled with
    content.
    """

    def mk_table(name, definition="id INT", content=None):
        with psycopg.ClientCursor(temp_db_conn) as cur:
            cur.execute("CREATE TABLE {} ({})".format(name, definition))
            if content:
                sql = pysql.SQL("INSERT INTO {} VALUES ({})").format(
                    pysql.Identifier(name),
                    pysql.SQL(",").join(
                        [pysql.Placeholder() for _ in range(len(content[0]))]
                    ),
                )
                cur.executemany(sql, content)

    return mk_table


@pytest.fixture
def def_config():
    cfg = Configuration(None)
    cfg.set_libdirs(osm2pgsql=None)
    return cfg


@pytest.fixture
def project_env(tmp_path):
    projdir = tmp_path / "project"
    projdir.mkdir()
    cfg = Configuration(projdir)
    cfg.set_libdirs(osm2pgsql=None)
    return cfg


@pytest.fixture
def property_table(table_factory, temp_db_conn):
    table_factory("nominatim_properties", "property TEXT, value TEXT")

    return mocks.MockPropertyTable(temp_db_conn)


@pytest.fixture
def status_table(table_factory):
    """Create an empty version of the status table and
    the status logging table.
    """
    table_factory(
        "import_status",
        """lastimportdate timestamp with time zone NOT NULL,
                     sequence_id integer,
                     indexed boolean""",
    )
    table_factory(
        "import_osmosis_log",
        """batchend timestamp,
                     batchseq integer,
                     batchsize bigint,
                     starttime timestamp,
                     endtime timestamp,
                     event text""",
    )


@pytest.fixture
def place_table(temp_db_with_extensions, table_factory):
    """Create an empty version of the place table."""
    table_factory(
        "place",
        """osm_id int8 NOT NULL,
                     osm_type char(1) NOT NULL,
                     class text NOT NULL,
                     type text NOT NULL,
                     name hstore,
                     admin_level smallint,
                     address hstore,
                     extratags hstore,
                     geometry Geometry(Geometry,4326) NOT NULL""",
    )


@pytest.fixture
def place_row(place_table, temp_db_cursor):
    """A factory for rows in the place table. The table is created as a
    prerequisite to the fixture.
    """
    idseq = itertools.count(1001)

    def _insert(
        osm_type="N",
        osm_id=None,
        cls="amenity",
        typ="cafe",
        names=None,
        admin_level=None,
        address=None,
        extratags=None,
        geom=None,
    ):
        temp_db_cursor.execute(
            "INSERT INTO place VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                osm_id or next(idseq),
                osm_type,
                cls,
                typ,
                names,
                admin_level,
                address,
                extratags,
                geom or "SRID=4326;POINT(0 0)",
            ),
        )

    return _insert


@pytest.fixture
def placex_table(temp_db_with_extensions, temp_db_conn):
    """Create an empty version of the place table."""
    return mocks.MockPlacexTable(temp_db_conn)


@pytest.fixture
def osmline_table(temp_db_with_extensions, table_factory):
    table_factory(
        "location_property_osmline",
        """place_id BIGINT,
                     osm_id BIGINT,
                     parent_place_id BIGINT,
                     geometry_sector INTEGER,
                     indexed_date TIMESTAMP,
                     startnumber INTEGER,
                     endnumber INTEGER,
                     partition SMALLINT,
                     indexed_status SMALLINT,
                     linegeo GEOMETRY,
                     interpolationtype TEXT,
                     address HSTORE,
                     postcode TEXT,
                     country_code VARCHAR(2)""",
    )


@pytest.fixture
def sql_preprocessor_cfg(tmp_path, table_factory, temp_db_with_extensions):
    table_factory("country_name", "partition INT", ((0,), (1,), (2,)))
    cfg = Configuration(None)
    cfg.set_libdirs(osm2pgsql=None, sql=tmp_path)
    return cfg


@pytest.fixture
def sql_preprocessor(sql_preprocessor_cfg, temp_db_conn):
    return SQLPreprocessor(temp_db_conn, sql_preprocessor_cfg)


@pytest.fixture
def tokenizer_mock(monkeypatch, property_table):
    """Sets up the configuration so that the test dummy tokenizer will be
    loaded when the tokenizer factory is used. Also returns a factory
    with which a new dummy tokenizer may be created.
    """
    monkeypatch.setenv("NOMINATIM_TOKENIZER", "dummy")

    def _import_dummy(*args, **kwargs):
        return dummy_tokenizer

    monkeypatch.setattr(
        nominatim_db.tokenizer.factory, "_import_tokenizer", _import_dummy
    )
    property_table.set("tokenizer", "dummy")

    def _create_tokenizer():
        return dummy_tokenizer.DummyTokenizer(None, None)

    return _create_tokenizer
