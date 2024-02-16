# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2023 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Functions for specialised logging with HTML output.
"""
from typing import Any, Iterator, Optional, List, Tuple, cast, Union, Mapping, Sequence
from contextvars import ContextVar
import datetime as dt
import textwrap
import io
import re
import html

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection

try:
    from pygments import highlight
    from pygments.lexers import PythonLexer, PostgresLexer
    from pygments.formatters import HtmlFormatter
    CODE_HIGHLIGHT = True
except ModuleNotFoundError:
    CODE_HIGHLIGHT = False


def _debug_name(res: Any) -> str:
    if res.names:
        return cast(str, res.names.get('name', next(iter(res.names.values()))))

    return f"Hnr {res.housenumber}" if res.housenumber is not None else '[NONE]'


class BaseLogger:
    """ Interface for logging function.

        The base implementation does nothing. Overwrite the functions
        in derived classes which implement logging functionality.
    """
    def get_buffer(self) -> str:
        """ Return the current content of the log buffer.
        """
        return ''

    def function(self, func: str, **kwargs: Any) -> None:
        """ Start a new debug chapter for the given function and its parameters.
        """


    def section(self, heading: str) -> None:
        """ Start a new section with the given title.
        """


    def comment(self, text: str) -> None:
        """ Add a simple comment to the debug output.
        """


    def var_dump(self, heading: str, var: Any) -> None:
        """ Print the content of the variable to the debug output prefixed by
            the given heading.
        """


    def table_dump(self, heading: str, rows: Iterator[Optional[List[Any]]]) -> None:
        """ Print the table generated by the generator function.
        """


    def result_dump(self, heading: str, results: Iterator[Tuple[Any, Any]]) -> None:
        """ Print a list of search results generated by the generator function.
        """


    def sql(self, conn: AsyncConnection, statement: 'sa.Executable',
            params: Union[Mapping[str, Any], Sequence[Mapping[str, Any]], None]) -> None:
        """ Print the SQL for the given statement.
        """

    def format_sql(self, conn: AsyncConnection, statement: 'sa.Executable',
                   extra_params: Union[Mapping[str, Any],
                                 Sequence[Mapping[str, Any]], None]) -> str:
        """ Return the comiled version of the statement.
        """
        compiled = cast('sa.ClauseElement', statement).compile(conn.sync_engine)

        params = dict(compiled.params)
        if isinstance(extra_params, Mapping):
            for k, v in extra_params.items():
                if hasattr(v, 'to_wkt'):
                    params[k] = v.to_wkt()
                elif isinstance(v, (int, float)):
                    params[k] = v
                else:
                    params[k] = str(v)
        elif isinstance(extra_params, Sequence) and extra_params:
            for k in extra_params[0]:
                params[k] = f':{k}'

        sqlstr = str(compiled)

        if conn.dialect.name == 'postgresql':
            if sa.__version__.startswith('1'):
                try:
                    sqlstr = re.sub(r'__\[POSTCOMPILE_[^]]*\]', '%s', sqlstr)
                    return sqlstr % tuple((repr(params.get(name, None))
                                          for name in compiled.positiontup)) # type: ignore
                except TypeError:
                    return sqlstr

            # Fixes an odd issue with Python 3.7 where percentages are not
            # quoted correctly.
            sqlstr = re.sub(r'%(?!\()', '%%', sqlstr)
            sqlstr = re.sub(r'__\[POSTCOMPILE_([^]]*)\]', r'%(\1)s', sqlstr)
            return sqlstr % params

        assert conn.dialect.name == 'sqlite'

        # params in positional order
        pparams = (repr(params.get(name, None)) for name in compiled.positiontup) # type: ignore

        sqlstr = re.sub(r'__\[POSTCOMPILE_([^]]*)\]', '?', sqlstr)
        sqlstr = re.sub(r"\?", lambda m: next(pparams), sqlstr)

        return sqlstr

class HTMLLogger(BaseLogger):
    """ Logger that formats messages in HTML.
    """
    def __init__(self) -> None:
        self.buffer = io.StringIO()


    def _timestamp(self) -> None:
        self._write(f'<p class="timestamp">[{dt.datetime.now()}]</p>')


    def get_buffer(self) -> str:
        return HTML_HEADER + self.buffer.getvalue() + HTML_FOOTER


    def function(self, func: str, **kwargs: Any) -> None:
        self._timestamp()
        self._write(f"<h1>Debug output for {func}()</h1>\n<p>Parameters:<dl>")
        for name, value in kwargs.items():
            self._write(f'<dt>{name}</dt><dd>{self._python_var(value)}</dd>')
        self._write('</dl></p>')


    def section(self, heading: str) -> None:
        self._timestamp()
        self._write(f"<h2>{heading}</h2>")


    def comment(self, text: str) -> None:
        self._timestamp()
        self._write(f"<p>{text}</p>")


    def var_dump(self, heading: str, var: Any) -> None:
        self._timestamp()
        if callable(var):
            var = var()

        self._write(f'<h5>{heading}</h5>{self._python_var(var)}')


    def table_dump(self, heading: str, rows: Iterator[Optional[List[Any]]]) -> None:
        self._timestamp()
        head = next(rows)
        assert head
        self._write(f'<table><thead><tr><th colspan="{len(head)}">{heading}</th></tr><tr>')
        for cell in head:
            self._write(f'<th>{cell}</th>')
        self._write('</tr></thead><tbody>')
        for row in rows:
            if row is not None:
                self._write('<tr>')
                for cell in row:
                    self._write(f'<td>{cell}</td>')
                self._write('</tr>')
        self._write('</tbody></table>')


    def result_dump(self, heading: str, results: Iterator[Tuple[Any, Any]]) -> None:
        """ Print a list of search results generated by the generator function.
        """
        self._timestamp()
        def format_osm(osm_object: Optional[Tuple[str, int]]) -> str:
            if not osm_object:
                return '-'

            t, i = osm_object
            if t == 'N':
                fullt = 'node'
            elif t == 'W':
                fullt = 'way'
            elif t == 'R':
                fullt = 'relation'
            else:
                return f'{t}{i}'

            return f'<a href="https://www.openstreetmap.org/{fullt}/{i}">{t}{i}</a>'

        self._write(f'<h5>{heading}</h5><p><dl>')
        total = 0
        for rank, res in results:
            self._write(f'<dt>[{rank:.3f}]</dt>  <dd>{res.source_table.name}(')
            self._write(f"{_debug_name(res)}, type=({','.join(res.category)}), ")
            self._write(f"rank={res.rank_address}, ")
            self._write(f"osm={format_osm(res.osm_object)}, ")
            self._write(f'cc={res.country_code}, ')
            self._write(f'importance={res.importance or float("nan"):.5f})</dd>')
            total += 1
        self._write(f'</dl><b>TOTAL:</b> {total}</p>')


    def sql(self, conn: AsyncConnection, statement: 'sa.Executable',
            params: Union[Mapping[str, Any], Sequence[Mapping[str, Any]], None]) -> None:
        self._timestamp()
        sqlstr = self.format_sql(conn, statement, params)
        if CODE_HIGHLIGHT:
            sqlstr = highlight(sqlstr, PostgresLexer(),
                               HtmlFormatter(nowrap=True, lineseparator='<br />'))
            self._write(f'<div class="highlight"><code class="lang-sql">{sqlstr}</code></div>')
        else:
            self._write(f'<code class="lang-sql">{html.escape(sqlstr)}</code>')


    def _python_var(self, var: Any) -> str:
        if CODE_HIGHLIGHT:
            fmt = highlight(str(var), PythonLexer(), HtmlFormatter(nowrap=True))
            return f'<div class="highlight"><code class="lang-python">{fmt}</code></div>'

        return f'<code class="lang-python">{html.escape(str(var))}</code>'


    def _write(self, text: str) -> None:
        """ Add the raw text to the debug output.
        """
        self.buffer.write(text)


class TextLogger(BaseLogger):
    """ Logger creating output suitable for the console.
    """
    def __init__(self) -> None:
        self.buffer = io.StringIO()


    def _timestamp(self) -> None:
        self._write(f'[{dt.datetime.now()}]\n')


    def get_buffer(self) -> str:
        return self.buffer.getvalue()


    def function(self, func: str, **kwargs: Any) -> None:
        self._write(f"#### Debug output for {func}()\n\nParameters:\n")
        for name, value in kwargs.items():
            self._write(f'  {name}: {self._python_var(value)}\n')
        self._write('\n')


    def section(self, heading: str) -> None:
        self._timestamp()
        self._write(f"\n# {heading}\n\n")


    def comment(self, text: str) -> None:
        self._write(f"{text}\n")


    def var_dump(self, heading: str, var: Any) -> None:
        if callable(var):
            var = var()

        self._write(f'{heading}:\n  {self._python_var(var)}\n\n')


    def table_dump(self, heading: str, rows: Iterator[Optional[List[Any]]]) -> None:
        self._write(f'{heading}:\n')
        data = [list(map(self._python_var, row)) if row else None for row in rows]
        assert data[0] is not None
        num_cols = len(data[0])

        maxlens = [max(len(d[i]) for d in data if d) for i in range(num_cols)]
        tablewidth = sum(maxlens) + 3 * num_cols + 1
        row_format = '| ' +' | '.join(f'{{:<{l}}}' for l in maxlens) + ' |\n'
        self._write('-'*tablewidth + '\n')
        self._write(row_format.format(*data[0]))
        self._write('-'*tablewidth + '\n')
        for row in data[1:]:
            if row:
                self._write(row_format.format(*row))
            else:
                self._write('-'*tablewidth + '\n')
        if data[-1]:
            self._write('-'*tablewidth + '\n')


    def result_dump(self, heading: str, results: Iterator[Tuple[Any, Any]]) -> None:
        self._timestamp()
        self._write(f'{heading}:\n')
        total = 0
        for rank, res in results:
            self._write(f'[{rank:.3f}]  {res.source_table.name}(')
            self._write(f"{_debug_name(res)}, type=({','.join(res.category)}), ")
            self._write(f"rank={res.rank_address}, ")
            self._write(f"osm={''.join(map(str, res.osm_object or []))}, ")
            self._write(f'cc={res.country_code}, ')
            self._write(f'importance={res.importance or -1:.5f})\n')
            total += 1
        self._write(f'TOTAL: {total}\n\n')


    def sql(self, conn: AsyncConnection, statement: 'sa.Executable',
            params: Union[Mapping[str, Any], Sequence[Mapping[str, Any]], None]) -> None:
        self._timestamp()
        sqlstr = '\n| '.join(textwrap.wrap(self.format_sql(conn, statement, params), width=78))
        self._write(f"| {sqlstr}\n\n")


    def _python_var(self, var: Any) -> str:
        return str(var)


    def _write(self, text: str) -> None:
        self.buffer.write(text)


logger: ContextVar[BaseLogger] = ContextVar('logger', default=BaseLogger())


def set_log_output(fmt: str) -> None:
    """ Enable collecting debug information.
    """
    if fmt == 'html':
        logger.set(HTMLLogger())
    elif fmt == 'text':
        logger.set(TextLogger())
    else:
        logger.set(BaseLogger())


def log() -> BaseLogger:
    """ Return the logger for the current context.
    """
    return logger.get()


def get_and_disable() -> str:
    """ Return the current content of the debug buffer and disable logging.
    """
    buf = logger.get().get_buffer()
    logger.set(BaseLogger())
    return buf


HTML_HEADER: str = """<!DOCTYPE html>
<html>
<head>
  <title>Nominatim - Debug</title>
  <style>
""" + \
(HtmlFormatter(nobackground=True).get_style_defs('.highlight') if CODE_HIGHLIGHT else '') +\
"""
    h2 { font-size: x-large }

    dl {
      padding-left: 10pt;
      font-family: monospace
    }

    dt {
      float: left;
      font-weight: bold;
      margin-right: 0.5em
    }

    dt::after { content: ": "; }

    dd::after {
      clear: left;
      display: block
    }

    .lang-sql {
      color: #555;
      font-size: small
    }

    h5 {
        border: solid lightgrey 0.1pt;
        margin-bottom: 0;
        background-color: #f7f7f7
    }

    h5 + .highlight {
        padding: 3pt;
        border: solid lightgrey 0.1pt
    }

    table, th, tbody {
        border: thin solid;
        border-collapse: collapse;
    }
    td {
        border-right: thin solid;
        padding-left: 3pt;
        padding-right: 3pt;
    }

    .timestamp {
        font-size: 0.8em;
        color: darkblue;
        width: calc(100% - 5pt);
        text-align: right;
        position: absolute;
        left: 0;
        margin-top: -5px;
    }
  </style>
</head>
<body>
"""

HTML_FOOTER: str = "</body></html>"
