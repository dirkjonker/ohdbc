import ctypes

from ohdbc.sql import *
from ohdbc.sqltypes import *
from ohdbc.utils import check_error, c_utf_16_le


class Cursor:
    def __init__(self, conn):
        """Return a database cursor"""
        self.conn = conn
        self.api = conn.api
        self.handle = ctypes.c_void_p()
        self.handle_type = SQL_HANDLE_STMT
        self.arraysize = 1
        self.stmt = None
        rc = self.conn.api.SQLAllocHandle(SQL_HANDLE_STMT, conn.handle,
                                          ctypes.byref(self.handle))
        check_error(self, rc, 'allocate statement handle')

    def set_options(self):
        """Set options for statement handle (cursor)"""
        self.c_arraysize = ctypes.c_long(self.arraysize)
        rc = self.conn.api.SQLSetStmtAttr(self.handle, SQL_ATTR_ROW_ARRAY_SIZE,
                                          self.c_arraysize, 0)
        check_error(self, rc, 'set row array size')

        self.rowstatus = (ctypes.c_short * self.arraysize)()
        p_rowstatus = ctypes.byref(self.rowstatus)
        rc = self.conn.api.SQLSetStmtAttr(self.handle, SQL_ATTR_ROW_STATUS_PTR,
                                          p_rowstatus, 0)
        check_error(self, rc, 'set rowstatus pointer')

        self.rows_fetched = ctypes.c_long()
        p_rows_fetched = ctypes.byref(self.rows_fetched)
        rc = self.conn.api.SQLSetStmtAttr(
            self.handle, SQL_ATTR_ROWS_FETCHED_PTR, p_rows_fetched, 0)
        check_error(self, rc, 'set rows_fetched pointer')

    def __enter__(self):
        return self.close()

    def __exit__(self, *args, **kwargs):
        return self.close()

    def close(self):
        """Close the cursor, free the handle"""
        del self.return_buffer
        rc = self.conn.api.SQLFreeHandle(SQL_HANDLE_STMT, self.handle)
        check_error(self, rc, 'free handle')
        self.closed = True

    def prepare(self, stmt):
        """Prepare statement"""
        self.set_options()
        stmt = bytes(stmt, 'utf-8')
        self.stmt = stmt
        c_stmt = ctypes.c_char_p(stmt)
        rc = self.conn.api.SQLPrepare(self.handle, c_stmt, len(stmt))
        check_error(self, rc, 'prepare stmt')

    def execute(self, stmt=None, params=None):
        """Execute (prepared) statement"""
        if stmt is not None and self.stmt is None:
            self.prepare(stmt)

        rc = self.conn.api.SQLExecute(self.handle)
        check_error(self, rc, 'execute')
        self.colcount = ctypes.c_short()

        rc = self.conn.api.SQLNumResultCols(self.handle,
                                            ctypes.byref(self.colcount))
        check_error(self, rc, 'get stmt column count')
        self._bindcols()

        # get rowcount
        self.rowcount = ctypes.c_long()
        rc = self.conn.api.SQLRowCount(self.handle,
                                       ctypes.byref(self.rowcount))
        check_error(self, rc, 'get stmt rowcount')
        return self

    def fetchmany(self, n=None):
        """Fetch the next (set of) row(s)"""
        if n is None:
            n = self.arraysize
        rc = self.conn.api.SQLFetch(self.handle)
        if rc == SQL_NO_DATA:
            return None
        check_error(self, rc, 'fetch')
        retcols = [[]] * self.colcount.value
        for j, col in enumerate(self.return_buffer):
            n_rows_to_fetch = range(self.rows_fetched.value)
            all_rows = []
            for i in n_rows_to_fetch:
                length = col[2][i]
                if length < 0:
                    all_rows.append(None)
                    continue
                if col[3]:  # is_char_array
                    value = col[1][i][:length]
                    all_rows.append(value.decode('utf_16_le'))
                else:
                    all_rows.append(col[1][i])
            retcols[j] = all_rows
        return zip(*retcols)

    def fetchall(self):
        rows = []
        while True:
            r = self.fetchmany()
            if not r:
                break
            rows.extend(list(r))
        return rows

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
        col_name = ctypes.create_string_buffer(256)
        col_name_size = ctypes.c_short()
        col_type = ctypes.c_short()
        col_type_size = ctypes.c_ssize_t()
        col_dec_digits = ctypes.c_short()
        col_nullable = ctypes.c_short()
        rc = self.conn.api.SQLDescribeColW(
            self.handle, col_num, ctypes.byref(col_name),
            ctypes.sizeof(col_name), ctypes.byref(col_name_size),
            ctypes.byref(col_type), ctypes.byref(col_type_size),
            ctypes.byref(col_dec_digits), ctypes.byref(col_nullable))
        check_error(self, rc, 'request col {}'.format(col_num))
        col_name_decoded = col_name[:col_name_size.value*2].decode('utf_16_le')
        nullable = bool(1-col_nullable.value)
        # print('col #{} name: {}, type: {}, size: {} nullable: {}'.format(
        #     col_num, col_name_decoded, col_type.value, col_type_size.value,
        #     nullable))
        c_col_type = SQL_TYPE_MAP[col_type.value]
        charsize = None
        is_char_array = False
        is_fixed_width = False
        if col_type.value in ALL_SQL_CHAR:
            is_char_array = True
            c_col_type = ctypes.c_char
            charsize = col_type_size.value + 1
            if col_type.value in (SQL_CHAR, SQL_WCHAR):
                is_fixed_width = True
                col_type.value = SQL_CHAR
            elif col_type.value in (SQL_WCHAR, SQL_WVARCHAR, SQL_WLONGVARCHAR):
                # ODBC Unicode != utf-8; can't use the ctypes c_wchar
                charsize = col_type_size.value * 2 + 2
                col_type.value = SQL_WCHAR
            col_buff = ((c_col_type * charsize) * self.arraysize)()
        else:
            col_buff = (c_col_type * self.arraysize)()
        if col_type.value == SQL_BIGINT:
            col_type.value = -25  # SQL_C_BIGINT
        col_indicator = (ctypes.c_ssize_t * self.arraysize)()
        self.return_buffer.append((col_num, col_buff, col_indicator,
                                   is_char_array, is_fixed_width, nullable))
        # Bind the column
        rc = self.conn.api.SQLBindCol(self.handle, col_num, col_type.value,
                                      ctypes.byref(col_buff), charsize,
                                      ctypes.byref(col_indicator))
        check_error(self, rc, 'bind col {}'.format(col_num))
