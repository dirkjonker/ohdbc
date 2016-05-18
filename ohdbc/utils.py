import ctypes

from ohdbc.sql import *


def check_error(obj, ret, handle_type=None, handle=None):
    """Validate return value and retrieve diagnostic info if applicable"""
    if ret in (SQL_SUCCESS, SQL_SUCCESS_WITH_INFO, SQL_NO_DATA):
        return

    if obj.handle_type in (SQL_HANDLE_DBC, SQL_HANDLE_ENV, SQL_HANDLE_STMT):
        sql_state = ctypes.create_string_buffer(10)
        native_error = ctypes.c_short()
        message_text = ctypes.create_string_buffer(500)
        message_length = ctypes.c_short()
        ODBC_API.SQLGetDiagRecW(
            handle_type, handle, 1, ctypes.byref(sql_state),
            ctypes.byref(native_error), ctypes.byref(message_text),
            ctypes.sizeof(message_text), ctypes.byref(message_length))
        error_msg = "[{}] {}".format(
            sql_state.raw.decode('utf_16_le'),
            message_text.raw.decode('utf_16_le'))
        raise utils.DatabaseError(error_msg)


def create_utf16_buffer(string):
    return ctypes.create_string_buffer(string.encode('utf_16_le') + b'\x00')


def bufread(array):
    return array.raw.decode('utf_16_le')


class Error(Exception):
    """Default Error as defined in DBAPI 2.0"""


class DatabaseError(Error):
    """Database Error as defined in DBAPI 2.0"""


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
    def __init__(self, value=None):
        super(c_utf_16_le, self).__init__()
        if value is not None:
            self.value = value

    @property
    def value(self,
              c_void_p=ctypes.c_void_p):
        addr = c_void_p.from_buffer(self).value
        return decode_utf16_from_address(addr)

    @value.setter
    def value(self, value,
              c_char=ctypes.c_char):
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
