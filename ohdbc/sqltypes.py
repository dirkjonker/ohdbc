import ctypes

from .sql import *

SQL_TYPE_MAP = {
    SQL_INTEGER: ctypes.c_int,
    SQL_CHAR: ctypes.c_char,
    SQL_WCHAR: ctypes.c_wchar,
    SQL_VARCHAR: ctypes.c_char,
    SQL_WVARCHAR: ctypes.c_wchar
}

# types from sqlext.h
SQL_ATTR_ODBC_VERSION = 200
SQL_OV_ODBC3 = ctypes.c_ulong(3)

# /* Options for SQLDriverConnect */
SQL_DRIVER_NOPROMPT = 0
SQL_DRIVER_COMPLETE = 1
SQL_DRIVER_PROMPT = 2
SQL_DRIVER_COMPLETE_REQUIRED = 3
