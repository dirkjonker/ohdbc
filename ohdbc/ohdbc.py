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

import ctypes
import time

from .sql import *
from .sqltypes import *

# not sure if I want these to be global
api = None
env_h = None


class Error(Exception):
    pass


class DatabaseError(Error):
    pass


def decode_utf16_from_address(address, c_char=ctypes.c_char):
    if not address:
        return None
    chars = b''
    while True:
        c1 = c_char.from_address(address).value
        c2 = c_char.from_address(address + 1).value
        if c1 == b'\x00' and c2 == b'\x00':
            break
        chars += c1
        chars += c2
        address += 2
    return chars.decode('utf_16_le')


class c_utf_16_le(ctypes.c_char):
    """ODBC unicode is not utf-8 encoded, so need some
    custom c-type since create_unicode_buffer is useless
    as it only works with utf-8

    This works on 64-bit linux but perhaps other platforms
    use a different encoding. Need to check.
    """

    def __init__(self, value=None):
        super(c_utf_16_le, self).__init__()
        if value is not None:
            self.value = value

    @property
    def value(self, c_void_p=ctypes.c_void_p):
        addr = c_void_p.from_buffer(self).value
        return decode_utf16_from_address(addr)

    @value.setter
    def value(self, value, c_char=ctypes.c_char):
        value = value.encode('utf-16le') + b'\x00'
        c_char.value.__set__(self, value)

    @classmethod
    def from_param(cls, obj):
        if isinstance(obj, unicode):
            obj = obj.encode('utf_16_le') + b'\x00'
        return super(c_utf16le_p, cls).from_param(obj)

    @classmethod
    def _check_retval_(cls, result):
        return result.value


def check_error(obj, rc, message):
    if rc in (SQL_SUCCESS, SQL_SUCCESS_WITH_INFO):
        return
    if obj is None:
        raise DatabaseError(message)
    msgbuffer = ctypes.create_string_buffer(1024)
    # try and get diagnostic info
    rc = api.SQLGetDiagField(obj.handle_type, obj.handle, 1,
                             SQL_DIAG_MESSAGE_TEXT, ctypes.byref(msgbuffer),
                             ctypes.sizeof(msgbuffer), None)
    raise DatabaseError('{} not succeeded: {}'.format(message,
                                                      msgbuffer.value))


def connect(connstr, **kwargs):
    """Create a connection to an ODBC data source"""
    return Connection(connstr, **kwargs)


def _init_env():
    """Initialize ODBC env handle
    Maybe add some more settings here, connection pooling etc.
    """
    global api
    global env_h
    api = ctypes.cdll.LoadLibrary('libodbc.so')
    env_h = ctypes.c_void_p()
    rc = api.SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE,
                            ctypes.byref(env_h))
    check_error(None, rc, 'set env handle')

    # set ODBC version to 3
    rc = api.SQLSetEnvAttr(env_h, SQL_ATTR_ODBC_VERSION, SQL_OV_ODBC3, 0)
    check_error(None, rc, 'set odbc version to 3')


class Cursor:
    def __init__(self, conn):
        """Return a database cursor"""
        self.conn = conn
        self.handle = ctypes.c_void_p()
        self.handle_type = SQL_HANDLE_STMT
        self.arraysize = 1
        self.stmt = None
        rc = api.SQLAllocHandle(SQL_HANDLE_STMT, conn.handle,
                                ctypes.byref(self.handle))
        check_error(self, rc, 'allocate statement handle')

    def set_options(self):
        """Set options for statement handle (cursor)"""
        self.c_arraysize = ctypes.c_long(self.arraysize)
        rc = api.SQLSetStmtAttr(self.handle, SQL_ATTR_ROW_ARRAY_SIZE,
                                self.c_arraysize, 0)
        check_error(self, rc, 'set row array size')

        self.rowstatus = (ctypes.c_short * self.arraysize)()
        p_rowstatus = ctypes.byref(self.rowstatus)
        rc = api.SQLSetStmtAttr(self.handle, SQL_ATTR_ROW_STATUS_PTR,
                                p_rowstatus, 0)
        check_error(self, rc, 'set rowstatus pointer')

        self.rows_fetched = ctypes.c_long()
        p_rows_fetched = ctypes.byref(self.rows_fetched)
        rc = api.SQLSetStmtAttr(self.handle, SQL_ATTR_ROWS_FETCHED_PTR,
                                p_rows_fetched, 0)
        check_error(self, rc, 'set rows_fetched pointer')

    def __enter__(self):
        return self.close()

    def __exit__(self, *args, **kwargs):
        return self.close()

    def close(self):
        """Close the cursor, free the handle"""
        del self.return_buffer
        rc = api.SQLFreeHandle(SQL_HANDLE_STMT, self.handle)
        check_error(self, rc, 'free handle')
        self.closed = True

    def prepare(self, stmt):
        """Prepare statement"""
        self.set_options()
        stmt = bytes(stmt, 'utf-8')
        self.stmt = stmt
        c_stmt = ctypes.c_char_p(stmt)
        rc = api.SQLPrepare(self.handle, c_stmt, len(stmt))
        check_error(self, rc, 'prepare stmt')

    def execute(self, stmt=None, params=None):
        """Execute (prepared) statement"""
        if stmt is not None and self.stmt is None:
            self.prepare(stmt)
        rc = api.SQLExecute(self.handle)
        check_error(self, rc, 'execute')
        self.colcount = ctypes.c_short()
        rc = api.SQLNumResultCols(self.handle, ctypes.byref(self.colcount))
        check_error(self, rc, 'get stmt column count')
        self._bindcols()
        # get rowcount
        self.rowcount = ctypes.c_long()
        rc = api.SQLRowCount(self.handle, ctypes.byref(self.rowcount))
        check_error(self, rc, 'get stmt rowcount')
        return self

    def fetch(self):
        """Fetch the next (set of) row(s)"""
        rc = api.SQLFetch(self.handle)
        if rc == SQL_NO_DATA:
            return None
        check_error(self, rc, 'fetch')
        retcols = [[]] * self.colcount.value
        for j, col in enumerate(self.return_buffer):
            n_rows_to_fetch = range(self.rows_fetched.value)
            if col[3]:  # is_char_array
                retcols[j] = [col[1][i][:col[2][i]].decode('utf_16_le')
                              for i in n_rows_to_fetch]
            else:
                retcols[j] = [col[1][i] for i in n_rows_to_fetch]
        return zip(*retcols)

    def _bindparams(self):
        """Bind all params"""
        raise NotImplementedError("Not yet implemented: parameter binding")

    def _bindparam(self):
        """Bind a parameter for a placeholder"""
        raise NotImplementedError("Not yet implemented")

    def _bindcols(self):
        """Loop over all cols and bind them"""
        self.return_buffer = []
        for col in range(1, self.colcount.value + 1):
            self._bindcol(col)

    def _bindcol(self, col_num):
        """Get col description and then bind the col"""
        col_name = ctypes.create_string_buffer(1024)
        col_name_size = ctypes.c_short()
        col_type = ctypes.c_short()
        col_type_size = ctypes.c_ssize_t()
        col_dec_digits = ctypes.c_short()
        col_nullable = ctypes.c_short()
        rc = api.SQLDescribeCol(
            self.handle, col_num, ctypes.byref(col_name),
            ctypes.sizeof(col_name), ctypes.byref(col_name_size),
            ctypes.byref(col_type), ctypes.byref(col_type_size),
            ctypes.byref(col_dec_digits), ctypes.byref(col_nullable))
        check_error(self, rc, 'request col {}'.format(col_num))
        print('col #{} name: {}, type: {}, size: {} nullable: {}'.format(
            col_num, col_name.value, col_type.value, col_type_size.value,
            col_nullable.value))
        c_col_type = SQL_TYPE_MAP[col_type.value]
        if col_type.value in (SQL_CHAR, SQL_VARCHAR, SQL_WCHAR, SQL_WVARCHAR):
            if col_type.value in (SQL_WCHAR, SQL_WVARCHAR):
                # ODBC Unicode != utf-8; can't use the ctypes unicode buffer
                c_col_type = c_utf_16_le
                col_type.value = SQL_WCHAR
                charsize = col_type_size.value + 8
            else:
                col_type.value = SQL_CHAR
                charsize = col_type_size.value + 1
            col_buff = ((c_col_type * charsize) * self.arraysize)()
            is_char_array = True
        else:
            charsize = None
            col_buff = (c_col_type * self.arraysize)()
            is_char_array = False
        col_indicator = (ctypes.c_ssize_t * self.arraysize)()
        self.return_buffer.append((col_num, col_buff, col_indicator,
                                   is_char_array))
        rc = api.SQLBindCol(self.handle, col_num, col_type.value,
                            ctypes.byref(col_buff), charsize,
                            ctypes.byref(col_indicator))
        check_error(self, rc, 'bind col {}'.format(col_num))


class Connection:
    def __init__(self, connstr, autocommit=False, **kwargs):
        """Create a connection to an ODBC data source"""
        global env_h
        if not env_h:
            _init_env()
        self.closed = False
        self.handle = ctypes.c_void_p()
        self.handle_type = SQL_HANDLE_DBC
        # allocate connection handle
        rc = api.SQLAllocHandle(SQL_HANDLE_DBC, env_h,
                                ctypes.byref(self.handle))
        check_error(self, rc, 'allocate dbc handle')
        # connect
        connstr = bytes(connstr, 'utf-8')
        rc = api.SQLDriverConnectW(self.handle, None, ctypes.c_char_p(connstr),
                                   len(connstr), None, 0, None,
                                   SQL_DRIVER_NOPROMPT)
        check_error(self, rc, 'connect (driver)')
        # set autocommit behavior
        rc = api.SQLSetConnectAttr(self.handle, SQL_ATTR_AUTOCOMMIT,
                                   SQL_AUTOCOMMIT_DEFAULT, SQL_IS_UINTEGER)
        check_error(self, rc, 'set autocommit')

    def __enter__(self):
        pass

    def __exit__(self, *args, **kwargs):
        return self.close()

    def close(self):
        """Disconnect from the data source and free the handle"""
        rc = api.SQLEndTran(SQL_HANDLE_DBC, self.handle, SQL_ROLLBACK)
        check_error(self, rc, 'rollback')
        rc = api.SQLDisconnect(self.handle)
        check_error(self, rc, 'disconnect')
        api.SQLFreeHandle(SQL_HANDLE_DBC, self.handle)
        check_error(self, rc, 'free dbc handle')
        self.closed = True

    def cursor(self):
        """Get a cursor for this connection"""
        return Cursor(self)
