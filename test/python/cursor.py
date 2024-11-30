# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2024 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Specialised psycopg cursor with shortcut functions useful for testing.
"""
import psycopg


class CursorForTesting(psycopg.Cursor):
    """Extension to the DictCursor class that provides execution
    short-cuts that simplify writing assertions.
    """

    def scalar(self, sql, params=None):
        """Execute a query with a single return value and return this value.
        Raises an assertion when not exactly one row is returned.
        """
        self.execute(sql, params)
        assert self.rowcount == 1
        return self.fetchone()[0]

    def row_set(self, sql, params=None):
        """Execute a query and return the result as a set of tuples.
        Fails when the SQL command returns duplicate rows.
        """
        self.execute(sql, params)

        result = set((tuple(row) for row in self))
        assert len(result) == self.rowcount

        return result

    def table_exists(self, table):
        """Check that a table with the given name exists in the database."""
        num = self.scalar(
            """SELECT count(*) FROM pg_tables
                             WHERE tablename = %s""",
            (table,),
        )
        return num == 1

    def index_exists(self, table, index):
        """Check that an indexwith the given name exists on the given table."""
        num = self.scalar(
            """SELECT count(*) FROM pg_indexes
                             WHERE tablename = %s and indexname = %s""",
            (table, index),
        )
        return num == 1

    def table_rows(self, table, where=None):
        """Return the number of rows in the given table."""
        if where is None:
            return self.scalar("SELECT count(*) FROM " + table)

        return self.scalar("SELECT count(*) FROM {} WHERE {}".format(table, where))
