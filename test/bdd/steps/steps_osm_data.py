# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2024 by the Nominatim developer community.
# For a full list of authors see the git log.
import os
import random
import tempfile
from pathlib import Path

from geometry_alias import ALIASES

from nominatim_db.tools.exec_utils import run_osm2pgsql
from nominatim_db.tools.replication import run_osm2pgsql_updates


def get_osm2pgsql_options(nominatim_env, fname, append):
    return dict(
        import_file=fname,
        osm2pgsql="osm2pgsql",
        osm2pgsql_cache=50,
        osm2pgsql_style=str(nominatim_env.get_test_config().get_import_style_file()),
        osm2pgsql_style_path=nominatim_env.get_test_config().config_dir,
        threads=1,
        dsn=nominatim_env.get_libpq_dsn(),
        flatnode_file="",
        tablespaces=dict(slim_data="", slim_index="", main_data="", main_index=""),
        append=append,
    )


def write_opl_file(opl, grid):
    """Create a temporary OSM file from OPL and return the file name. It is
    the responsibility of the caller to delete the file again.

    Node with missing coordinates, can retrieve their coordinates from
    a supplied grid. Failing that a random coordinate is assigned.
    """
    with tempfile.NamedTemporaryFile(suffix=".opl", delete=False) as fd:
        for line in opl.splitlines():
            if line.startswith("n") and line.find(" x") < 0:
                coord = grid.grid_node(int(line[1:].split(" ")[0]))
                if coord is None:
                    coord = (random.random() * 360 - 180, random.random() * 180 - 90)
                line += " x%f y%f" % coord
            fd.write(line.encode("utf-8"))
            fd.write(b"\n")

        return fd.name


@given("the lua style file")
def lua_style_file(context):
    """Define a custom style file to use for the import."""
    style = Path(context.nominatim.website_dir.name) / "custom.lua"
    style.write_text(context.text)
    context.nominatim.test_env["NOMINATIM_IMPORT_STYLE"] = str(style)


@given("the ([0-9.]+ )?grid(?: with origin (?P<origin>.*))?")
def define_node_grid(context, grid_step, origin):
    """
    Define a grid of node positions.
    Use a table to define the grid. The nodes must be integer ids. Optionally
    you can give the grid distance. The default is 0.00001 degrees.
    """
    if grid_step is not None:
        grid_step = float(grid_step.strip())
    else:
        grid_step = 0.00001

    if origin:
        if "," in origin:
            # TODO coordinate
            coords = origin.split(",")
            if len(coords) != 2:
                raise RuntimeError("Grid origin expects origin with x,y coordinates.")
            origin = (float(coords[0]), float(coords[1]))
        elif origin in ALIASES:
            origin = ALIASES[origin]
        else:
            raise RuntimeError("Grid origin must be either coordinate or alias.")
    else:
        origin = (0.0, 0.0)

    context.osm.set_grid(
        [context.table.headings] + [list(h) for h in context.table], grid_step, origin
    )


@when("loading osm data")
def load_osm_file(context):
    """
    Load the given data into a freshly created test data using osm2pgsql.
    No further indexing is done.

    The data is expected as attached text in OPL format.
    """
    # create an OSM file and import it
    fname = write_opl_file(context.text, context.osm)
    try:
        run_osm2pgsql(get_osm2pgsql_options(context.nominatim, fname, append=False))
    finally:
        os.remove(fname)

    # reintroduce the triggers/indexes we've lost by having osm2pgsql set up place again
    cur = context.db.cursor()
    cur.execute(
        """CREATE TRIGGER place_before_delete BEFORE DELETE ON place
                    FOR EACH ROW EXECUTE PROCEDURE place_delete()"""
    )
    cur.execute(
        """CREATE TRIGGER place_before_insert BEFORE INSERT ON place
                   FOR EACH ROW EXECUTE PROCEDURE place_insert()"""
    )
    cur.execute(
        """CREATE UNIQUE INDEX idx_place_osm_unique on place using btree(osm_id,osm_type,class,type)"""
    )
    context.db.commit()


@when("updating osm data")
def update_from_osm_file(context):
    """
    Update a database previously populated with 'loading osm data'.
    Needs to run indexing on the existing data first to yield the correct result.

    The data is expected as attached text in OPL format.
    """
    context.nominatim.copy_from_place(context.db)
    context.nominatim.run_nominatim("index")
    context.nominatim.run_nominatim("refresh", "--functions")

    # create an OSM file and import it
    fname = write_opl_file(context.text, context.osm)
    try:
        run_osm2pgsql_updates(
            context.db, get_osm2pgsql_options(context.nominatim, fname, append=True)
        )
    finally:
        os.remove(fname)


@when("indexing")
def index_database(context):
    """
    Run the Nominatim indexing step. This will process data previously
    loaded with 'updating osm data'
    """
    context.nominatim.run_nominatim("index")
