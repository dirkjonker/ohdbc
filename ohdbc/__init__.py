"""
OH!dbc

Copyright 2016 Dirk Jonker
dirkjonker@gmail.com

A super minimal ODBC implementation to test performance for
different values of SQL_ATTR_ROW_ARRAY_SIZE, which determines
how many rows will be fetched for each network call.

It should greatly reduce overhead and improve data transfer speed
when catching many rows (e.g. for ETL operations)

Not compatible with anything
No support for anything

Inspired by pypyodbc, ceODBC and tutorials at Easysoft.com
"""


from ohdbc.connection import Connection


def connect(connstr, **kwargs):
    """Create a connection to an ODBC data source"""
    return Connection(connstr, **kwargs)
