import ctypes
import time

import ohdbc.utils as utils
from ohdbc.cursor import Cursor
from ohdbc.sql import *
from ohdbc.sqltypes import *
from ohdbc.utils import check_error

def _init_env():
    """Initialize ODBC env handle
    Maybe add some more settings here, connection pooling etc.
    """
    api = ctypes.cdll.LoadLibrary('libodbc.so')
    env_h = ctypes.c_void_p()
    rc = api.SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE,
                            ctypes.byref(env_h))
    check_error(None, rc, 'set env handle')

    # set ODBC version to 3
    rc = api.SQLSetEnvAttr(env_h, SQL_ATTR_ODBC_VERSION, SQL_OV_ODBC3, 0)
    check_error(None, rc, 'set odbc version to 3')
    return env_h, api


class Connection:
    def __init__(self, connstr, autocommit=False, **kwargs):
        """Create a connection to an ODBC data source"""
        self.env_h, self.api = _init_env()
        self.closed = False
        self.handle = ctypes.c_void_p()
        self.handle_type = SQL_HANDLE_DBC
        # allocate connection handle
        rc = self.api.SQLAllocHandle(SQL_HANDLE_DBC, self.env_h,
                                ctypes.byref(self.handle))
        check_error(self, rc, 'allocate dbc handle')
        # connect
        connstr = utils.create_utf16_buffer(connstr)
        rc = self.api.SQLDriverConnectW(self.handle, None, ctypes.byref(connstr),
                                   ctypes.sizeof(connstr), None, 0, None,
                                   SQL_DRIVER_NOPROMPT)
        check_error(self, rc, 'connect (driver)')
        # set autocommit behavior
        rc = self.api.SQLSetConnectAttr(self.handle, SQL_ATTR_AUTOCOMMIT,
                                   SQL_AUTOCOMMIT_DEFAULT, SQL_IS_UINTEGER)
        check_error(self, rc, 'set autocommit')

    def __enter__(self):
        pass

    def __exit__(self, *args, **kwargs):
        return self.close()

    def close(self):
        """Disconnect from the data source and free the handle"""
        rc = self.api.SQLEndTran(SQL_HANDLE_DBC, self.handle, SQL_ROLLBACK)
        check_error(self, rc, 'rollback')
        rc = self.api.SQLDisconnect(self.handle)
        check_error(self, rc, 'disconnect')
        rc = self.api.SQLFreeHandle(SQL_HANDLE_DBC, self.handle)
        check_error(self, rc, 'free dbc handle')
        self.closed = True

    def cursor(self):
        """Get a cursor for this connection"""
        return Cursor(self)
