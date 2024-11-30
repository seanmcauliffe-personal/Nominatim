# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2024 by the Nominatim developer community.
# For a full list of authors see the git log.
""" Steps that run queries against the API.
"""
from pathlib import Path
import re
import logging
import asyncio
import xml.etree.ElementTree as ET

from http_responses import (
    GenericResponse,
    SearchResponse,
    ReverseResponse,
    StatusResponse,
)
from check_functions import Bbox, check_for_attributes
from table_compare import NominatimID

LOG = logging.getLogger(__name__)


def make_todo_list(context, result_id):
    if result_id is None:
        context.execute_steps("then at least 1 result is returned")
        return range(len(context.response.result))

    context.execute_steps(f"then more than {result_id}results are returned")
    return (int(result_id.strip()),)


def compare(operator, op1, op2):
    if operator == "less than":
        return op1 < op2
    elif operator == "more than":
        return op1 > op2
    elif operator == "exactly":
        return op1 == op2
    elif operator == "at least":
        return op1 >= op2
    elif operator == "at most":
        return op1 <= op2
    else:
        raise ValueError(f"Unknown operator '{operator}'")


def send_api_query(endpoint, params, fmt, context):
    if fmt is not None:
        if fmt.strip() == "debug":
            params["debug"] = "1"
        else:
            params["format"] = fmt.strip()

    if context.table:
        if context.table.headings[0] == "param":
            for line in context.table:
                params[line["param"]] = line["value"]
        else:
            for h in context.table.headings:
                params[h] = context.table[0][h]

    return asyncio.run(
        context.nominatim.api_engine(
            endpoint,
            params,
            Path(context.nominatim.website_dir.name),
            context.nominatim.test_env,
            getattr(context, "http_headers", {}),
        )
    )


@given("the HTTP header")
def add_http_header(context):
    if not hasattr(context, "http_headers"):
        context.http_headers = {}

    for h in context.table.headings:
        context.http_headers[h] = context.table[0][h]


@when('sending (?P<fmt>\S+ )?search query "(?P<query>.*)"(?P<addr> with address)?')
def website_search_request(context, fmt, query, addr):
    params = {}
    if query:
        params["q"] = query
    if addr is not None:
        params["addressdetails"] = "1"

    outp, status = send_api_query("search", params, fmt, context)

    context.response = SearchResponse(outp, fmt or "json", status)


@when(
    "sending v1/reverse at (?P<lat>[\d.-]*),(?P<lon>[\d.-]*)(?: with format (?P<fmt>.+))?"
)
def api_endpoint_v1_reverse(context, lat, lon, fmt):
    params = {}
    if lat is not None:
        params["lat"] = lat
    if lon is not None:
        params["lon"] = lon
    if fmt is None:
        fmt = "jsonv2"
    elif fmt == "''":
        fmt = None

    outp, status = send_api_query("reverse", params, fmt, context)
    context.response = ReverseResponse(outp, fmt or "xml", status)


@when("sending v1/reverse N(?P<nodeid>\d+)(?: with format (?P<fmt>.+))?")
def api_endpoint_v1_reverse_from_node(context, nodeid, fmt):
    params = {}
    params["lon"], params["lat"] = (
        f"{c:f}" for c in context.osm.grid_node(int(nodeid))
    )

    outp, status = send_api_query("reverse", params, fmt, context)
    context.response = ReverseResponse(outp, fmt or "xml", status)


@when("sending (?P<fmt>\S+ )?details query for (?P<query>.*)")
def website_details_request(context, fmt, query):
    params = {}
    if query[0] in "NWR":
        nid = NominatimID(query)
        params["osmtype"] = nid.typ
        params["osmid"] = nid.oid
        if nid.cls:
            params["class"] = nid.cls
    else:
        params["place_id"] = query
    outp, status = send_api_query("details", params, fmt, context)

    context.response = GenericResponse(outp, fmt or "json", status)


@when("sending (?P<fmt>\S+ )?lookup query for (?P<query>.*)")
def website_lookup_request(context, fmt, query):
    params = {"osm_ids": query}
    outp, status = send_api_query("lookup", params, fmt, context)

    context.response = SearchResponse(outp, fmt or "xml", status)


@when("sending (?P<fmt>\S+ )?status query")
def website_status_request(context, fmt):
    params = {}
    outp, status = send_api_query("status", params, fmt, context)

    context.response = StatusResponse(outp, fmt or "text", status)


@step(
    "(?P<operator>less than|more than|exactly|at least|at most) (?P<number>\d+) results? (?:is|are) returned"
)
def validate_result_number(context, operator, number):
    context.execute_steps("Then a HTTP 200 is returned")
    numres = len(context.response.result)
    assert compare(
        operator, numres, int(number)
    ), f"Bad number of results: expected {operator} {number}, got {numres}."


@then("a HTTP (?P<status>\d+) is returned")
def check_http_return_status(context, status):
    assert context.response.errorcode == int(status), (
        f"Return HTTP status is {context.response.errorcode}."
        f" Full response:\n{context.response.page}"
    )


@then('the page contents equals "(?P<text>.+)"')
def check_page_content_equals(context, text):
    assert context.response.page == text


@then("the result is valid (?P<fmt>\w+)")
def step_impl(context, fmt):
    context.execute_steps("Then a HTTP 200 is returned")
    if fmt.strip() == "html":
        try:
            tree = ET.fromstring(context.response.page)
        except Exception as ex:
            assert False, f"Could not parse page: {ex}\n{context.response.page}"

        assert tree.tag == "html"
        body = tree.find("./body")
        assert body is not None
        assert body.find(".//script") is None
    else:
        assert context.response.format == fmt


@then("a (?P<fmt>\w+) user error is returned")
def check_page_error(context, fmt):
    context.execute_steps("Then a HTTP 400 is returned")
    assert context.response.format == fmt

    if fmt == "xml":
        assert (
            re.search(r"<error>.+</error>", context.response.page, re.DOTALL)
            is not None
        )
    else:
        assert re.search(r'({"error":)', context.response.page, re.DOTALL) is not None


@then("result header contains")
def check_header_attr(context):
    context.execute_steps("Then a HTTP 200 is returned")
    for line in context.table:
        assert (
            line["attr"] in context.response.header
        ), f"Field '{line['attr']}' missing in header. Full header:\n{context.response.header}"
        value = context.response.header[line["attr"]]
        assert (
            re.fullmatch(line["value"], value) is not None
        ), f"Attribute '{line['attr']}': expected: '{line['value']}', got '{value}'"


@then("result header has (?P<neg>not )?attributes (?P<attrs>.*)")
def check_header_no_attr(context, neg, attrs):
    check_for_attributes(context.response.header, attrs, "absent" if neg else "present")


@then("results contain(?: in field (?P<field>.*))?")
def step_impl(context, field):
    context.execute_steps("then at least 1 result is returned")

    for line in context.table:
        context.response.match_row(line, context=context, field=field)


@then("result (?P<lid>\d+ )?has (?P<neg>not )?attributes (?P<attrs>.*)")
def validate_attributes(context, lid, neg, attrs):
    for i in make_todo_list(context, lid):
        check_for_attributes(
            context.response.result[i], attrs, "absent" if neg else "present"
        )


@then("result addresses contain")
def step_impl(context):
    context.execute_steps("then at least 1 result is returned")

    for line in context.table:
        idx = int(line["ID"]) if "ID" in line.headings else None

        for name, value in zip(line.headings, line.cells):
            if name != "ID":
                context.response.assert_address_field(idx, name, value)


@then("address of result (?P<lid>\d+) has(?P<neg> no)? types (?P<attrs>.*)")
def check_address(context, lid, neg, attrs):
    context.execute_steps(f"then more than {lid} results are returned")

    addr_parts = context.response.result[int(lid)]["address"]

    for attr in attrs.split(","):
        if neg:
            assert attr not in addr_parts
        else:
            assert attr in addr_parts


@then("address of result (?P<lid>\d+) (?P<complete>is|contains)")
def check_address(context, lid, complete):
    context.execute_steps(f"then more than {lid} results are returned")

    lid = int(lid)
    addr_parts = dict(context.response.result[lid]["address"])

    for line in context.table:
        context.response.assert_address_field(lid, line["type"], line["value"])
        del addr_parts[line["type"]]

    if complete == "is":
        assert len(addr_parts) == 0, f"Additional address parts found: {addr_parts!s}"


@then("result (?P<lid>\d+ )?has bounding box in (?P<coords>[\d,.-]+)")
def check_bounding_box_in_area(context, lid, coords):
    expected = Bbox(coords)

    for idx in make_todo_list(context, lid):
        res = context.response.result[idx]
        check_for_attributes(res, "boundingbox")
        context.response.check_row(
            idx, res["boundingbox"] in expected, f"Bbox is not contained in {expected}"
        )


@then("result (?P<lid>\d+ )?has centroid in (?P<coords>[\d,.-]+)")
def check_centroid_in_area(context, lid, coords):
    expected = Bbox(coords)

    for idx in make_todo_list(context, lid):
        res = context.response.result[idx]
        check_for_attributes(res, "lat,lon")
        context.response.check_row(
            idx,
            (res["lon"], res["lat"]) in expected,
            f"Centroid is not inside {expected}",
        )


@then("there are(?P<neg> no)? duplicates")
def check_for_duplicates(context, neg):
    context.execute_steps("then at least 1 result is returned")

    resarr = set()
    has_dupe = False

    for res in context.response.result:
        dup = (res["osm_type"], res["class"], res["type"], res["display_name"])
        if dup in resarr:
            has_dupe = True
            break
        resarr.add(dup)

    if neg:
        assert not has_dupe, f"Found duplicate for {dup}"
    else:
        assert has_dupe, "No duplicates found"
