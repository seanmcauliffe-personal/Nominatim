# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2024 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Legacy word table for testing with functions to prefil and test contents
of the table.
"""
from nominatim_db.db.connection import execute_scalar


class MockIcuWordTable:
    """A word table for testing using legacy word table structure."""

    def __init__(self, conn):
        self.conn = conn
        with conn.cursor() as cur:
            cur.execute(
                """CREATE TABLE word (word_id INTEGER,
                                              word_token text NOT NULL,
                                              type text NOT NULL,
                                              word text,
                                              info jsonb)"""
            )

        conn.commit()

    def add_full_word(self, word_id, word, word_token=None):
        """Add a new word to the database.

        This method inserts a new entry into the `word` table with the provided `word_id`, `word`,
        and an optional `word_token`.
        The `type` field is set to `'W'`, and the `info` field is initialized as an empty JSONB
        object.

        Parameters:
            word_id (int): The unique identifier for the word.
            word (str): The word to be added.
            word_token (str, optional): An optional token for the word. Defaults to `None`.

        Raises:
            DatabaseError: If an error occurs while inserting the word into the database.
        """

        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO word (word_id, word_token, type, word, info)
                           VALUES(%s, %s, 'W', %s, '{}'::jsonb)""",
                (word_id, word or word_token, word),
            )
        self.conn.commit()

    def add_special(self, word_token, word, cls, typ, oper):
        """Add a special word to the database.

        This method inserts a new entry into the `word` table with the provided `word_token`,
        `word`,
        and additional information specified by `cls`, `typ`, and `oper`. The `type` field is set
        to `'S'`,
        and the `info` field is a JSON object containing `class`, `type`, and `op`.

        Parameters:
            word_token (str): The token representing the word.
            word (str): The word to be added.
            cls (str): The class of the special word.
            typ (str): The type of the special word.
            oper (str): The operator associated with the special word.

        Raises:
            DatabaseError: If an error occurs while inserting into the database.
        """

        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO word (word_token, type, word, info)
                              VALUES (%s, 'S', %s,
                                      json_build_object('class', %s::text,
                                                        'type', %s::text,
                                                        'op', %s::text))
                        """,
                (word_token, word, cls, typ, oper),
            )
        self.conn.commit()

    def add_country(self, country_code, word_token):
        """Add a country to the database.

        This method inserts a new entry into the `word` table with the provided `country_code`
        and `word_token`. The `type` field is set to `'C'`.

        Parameters:
            country_code (str): The country code to be added.
            word_token (str): The token representing the country.

        Raises:
            DatabaseError: If an error occurs while inserting into the database.
        """

        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO word (word_token, type, word)
                           VALUES(%s, 'C', %s)""",
                (word_token, country_code),
            )
        self.conn.commit()

    def add_postcode(self, word_token, postcode):
        """Add a postcode to the database.

        This method inserts a new entry into the `word` table with the provided `postcode`
        and `word_token`. The `type` field is set to `'P'`.

        Parameters:
            word_token (str): The token representing the postcode.
            postcode (str): The postcode to be added.

        Raises:
            DatabaseError: If an error occurs while inserting into the database.
        """

        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO word (word_token, type, word)
                              VALUES (%s, 'P', %s)
                        """,
                (word_token, postcode),
            )
        self.conn.commit()

    def add_housenumber(self, word_id, word_tokens, word=None):
        """Add a house number to the database.

        This method inserts a new entry into the `word` table with the provided `word_id`,
        `word_tokens`, and an optional `word`. If `word_tokens` is a string, it inserts a single
        entry with type 'H'. If `word_tokens` is a list, it inserts multiple entries with type
        'H' and associated `word` and `info` fields.

        Parameters:
            word_id (int): The unique identifier for the house number.
            word_tokens (str or list of str): The token(s) representing the house number.
            word (str, optional): The house number. Defaults to `None`.

        Raises:
            DatabaseError: If an error occurs while inserting into the database.
        """

        with self.conn.cursor() as cur:
            if isinstance(word_tokens, str):
                # old style without analyser
                cur.execute(
                    """INSERT INTO word (word_id, word_token, type)
                                  VALUES (%s, %s, 'H')
                            """,
                    (word_id, word_tokens),
                )
            else:
                if word is None:
                    word = word_tokens[0]
                for token in word_tokens:
                    cur.execute(
                        """INSERT INTO word (word_id, word_token, type, word, info)
                                      VALUES (%s, %s, 'H', %s, jsonb_build_object(
                                          'lookup', %s::text))
                                """,
                        (word_id, token, word, word_tokens[0]),
                    )

        self.conn.commit()

    def count(self):
        """Count the total number of entries in the `word` table.

        This method executes a scalar query to count the number of rows in the `word` table.

        Returns:
            int: The total count of entries in the `word` table.

        Raises:
            DatabaseError: If an error occurs while executing the query.
        """

        return execute_scalar(self.conn, "SELECT count(*) FROM word")

    def count_special(self):
        """Count the total number of special entries in the `word` table.

        This method executes a scalar query to count the number of rows in the `word` table
        where the `type` field is `'S'`.

        Returns:
            int: The total count of special entries in the `word` table.

        Raises:
            DatabaseError: If an error occurs while executing the query.
        """

        return execute_scalar(self.conn, "SELECT count(*) FROM word WHERE type = 'S'")

    def count_housenumbers(self):
        """Count the total number of house number entries in the `word` table.

        This method executes a scalar query to count the number of rows in the `word` table
        where the `type` field is `'H'`.

        Returns:
            int: The total count of house number entries in the `word` table.

        Raises:
            DatabaseError: If an error occurs while executing the query.
        """

        return execute_scalar(self.conn, "SELECT count(*) FROM word WHERE type = 'H'")

    def get_special(self):
        """Retrieve special entries from the database.

        This method queries the `word` table to fetch all rows where the `type` field is `'S'`.
        It returns a set of tuples, each containing `word_token`, `word`, `class`, `type`,
        and `op` values from the `info` JSONB object. An assertion ensures no duplicate entries.

        Returns:
            set: A set of tuples, where each tuple contains:
                - word_token (str): The token representing the word.
                - word (str): The word itself.
                - class (str): The class of the special entry from the `info` field.
                - type (str): The type of the special entry from the `info` field.
                - op (str): The operator of the special entry from the `info` field.

        Raises:
            AssertionError: If duplicate entries are found in the `word` table.
            DatabaseError: If an error occurs while executing the query.
        """

        with self.conn.cursor() as cur:
            cur.execute("SELECT word_token, info, word FROM word WHERE type = 'S'")
            result = set(
                (
                    (row[0], row[2], row[1]["class"], row[1]["type"], row[1]["op"])
                    for row in cur
                )
            )
            assert len(result) == cur.rowcount, "Word table has duplicates."
            return result

    def get_country(self):
        """Retrieve countries from the database.

        This method queries the `word` table to fetch all rows where the `type` field is `'C'`.
        It returns a set of tuples, each containing the `word` and `word_token`.
        An assertion is included to ensure there are no duplicate entries.

        Returns:
            set: A set of tuples, where each tuple contains a `word` (str) and
            its associated `word_token` (str).

        Raises:
            AssertionError: If duplicate entries are found in the `word` table.
            DatabaseError: If an error occurs while executing the query.
        """

        with self.conn.cursor() as cur:
            cur.execute("SELECT word, word_token FROM word WHERE type = 'C'")
            result = set((tuple(row) for row in cur))
            assert len(result) == cur.rowcount, "Word table has duplicates."
            return result

    def get_postcodes(self):
        """Retrieve postcodes from the database.

        This method queries the `word` table to fetch all rows where the `type` field is `'P'`.
        It returns a set of postcodes.

        Returns:
            set: A set of postcodes (str) retrieved from the database.

        Raises:
            DatabaseError: If an error occurs while executing the query.
        """

        with self.conn.cursor() as cur:
            cur.execute("SELECT word FROM word WHERE type = 'P'")
            return set((row[0] for row in cur))

    def get_partial_words(self):
        """Retrieve partial words from the database.

        This method queries the `word` table to fetch all rows where the `type` field is `'w'`.
        It returns a set of tuples, each containing the `word_token` and the `count` field
        from the `info` JSONB object.

        Returns:
            set: A set of tuples, where each tuple contains a `word_token` (str) and
            its count (int).

        Raises:
            DatabaseError: If an error occurs while executing the query.
        """

        with self.conn.cursor() as cur:
            cur.execute("SELECT word_token, info FROM word WHERE type ='w'")
            return set(((row[0], row[1]["count"]) for row in cur))
