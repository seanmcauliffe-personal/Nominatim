# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2024 by the Nominatim developer community.
# For a full list of authors see the git log.
import pytest

from nominatim_db.data.place_info import PlaceInfo
from nominatim_db.tokenizer.place_sanitizer import PlaceSanitizer


class TestTagJapanese:
    @pytest.fixture(autouse=True)
    def setup_country(self, def_config):
        self.config = def_config

    def run_sanitizer_on(self, type, **kwargs):
        place = PlaceInfo({"address": kwargs, "country_code": "jp"})
        sanitizer_args = {"step": "tag-japanese"}
        _, address = PlaceSanitizer([sanitizer_args], self.config).process_names(place)
        tmp_list = [(p.name, p.kind) for p in address]
        return sorted(tmp_list)

    def test_on_address(self):
        res = self.run_sanitizer_on("address", name="foo", ref="bar", ref_abc="baz")
        assert res == [("bar", "ref"), ("baz", "ref_abc"), ("foo", "name")]

    def test_housenumber(self):
        res = self.run_sanitizer_on("address", housenumber="2")
        assert res == [("2", "housenumber")]

    def test_blocknumber(self):
        res = self.run_sanitizer_on("address", block_number="6")
        assert res == [("6", "housenumber")]

    def test_neighbourhood(self):
        res = self.run_sanitizer_on("address", neighbourhood="8")
        assert res == [("8", "place")]

    def test_quarter(self):
        res = self.run_sanitizer_on("address", quarter="kase")
        assert res == [("kase", "place")]

    def test_housenumber_blocknumber(self):
        res = self.run_sanitizer_on("address", housenumber="2", block_number="6")
        assert res == [("6-2", "housenumber")]

    def test_quarter_neighbourhood(self):
        res = self.run_sanitizer_on("address", quarter="kase", neighbourhood="8")
        assert res == [("kase8", "place")]

    def test_blocknumber_housenumber_quarter(self):
        res = self.run_sanitizer_on(
            "address", block_number="6", housenumber="2", quarter="kase"
        )
        assert res == [("6-2", "housenumber"), ("kase", "place")]

    def test_blocknumber_housenumber_quarter_neighbourhood(self):
        res = self.run_sanitizer_on(
            "address", block_number="6", housenumber="2", neighbourhood="8"
        )
        assert res == [("6-2", "housenumber"), ("8", "place")]

    def test_blocknumber_quarter_neighbourhood(self):
        res = self.run_sanitizer_on(
            "address", block_number="6", quarter="kase", neighbourhood="8"
        )
        assert res == [("6", "housenumber"), ("kase8", "place")]

    def test_blocknumber_quarter(self):
        res = self.run_sanitizer_on("address", block_number="6", quarter="kase")
        assert res == [("6", "housenumber"), ("kase", "place")]

    def test_blocknumber_neighbourhood(self):
        res = self.run_sanitizer_on("address", block_number="6", neighbourhood="8")
        assert res == [("6", "housenumber"), ("8", "place")]

    def test_housenumber_quarter_neighbourhood(self):
        res = self.run_sanitizer_on(
            "address", housenumber="2", quarter="kase", neighbourhood="8"
        )
        assert res == [("2", "housenumber"), ("kase8", "place")]

    def test_housenumber_quarter(self):
        res = self.run_sanitizer_on("address", housenumber="2", quarter="kase")
        assert res == [("2", "housenumber"), ("kase", "place")]

    def test_housenumber_blocknumber_neighbourhood_quarter(self):
        res = self.run_sanitizer_on(
            "address",
            block_number="6",
            housenumber="2",
            quarter="kase",
            neighbourhood="8",
        )
        assert res == [("6-2", "housenumber"), ("kase8", "place")]
