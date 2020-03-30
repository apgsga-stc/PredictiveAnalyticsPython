"""
Connecting to APG Oracle Instances

@author: kpf
"""

import cx_Oracle as orcl
import pandas as pd

from pa_lib.log import time_log
from pa_lib.const import PA_ORA_CONN, PA_ORA_DSN_TEMPL


class Connection:
    """
    If only "instance" is given, connection parameters are looked up in const.PA_ORA_CONN.
    Instance will auto-close on going out of scope.
    """

    def __init__(
        self, instance: str, user: str = None, passwd: str = None, do_open: bool = False
    ):
        """Initializes connection parameters, opens connection if "do_open" is True."""
        self.connection = None
        self.cursor = None
        if user is None and passwd is None:
            (self.instance, self.user, self.passwd) = PA_ORA_CONN[instance].astuple()
        else:
            self.instance = instance
            self.user = user
            self.passwd = passwd if (passwd is not None) else user + "_pass"
        if do_open:
            self.open()

    # Context Manager Code
    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *exception_info):
        self.close()

    # --------------------------------------------------------------------------------
    # Public methods
    # --------------------------------------------------------------------------------

    # --------------------------------------------------------------------------------
    # Connection
    def open(self):
        """Open the connection, throw Exception if already open. Open cursor, enable DBMS_OUTPUT."""
        if self.connection is not None:
            raise RuntimeError(f"Already connected to {self.instance}")
        self.connection = self._connect(self.instance, self.user, self.passwd)
        self.cursor = self.connection.cursor()
        self.exec("dbms_output.enable")

    def close(self):
        """Close the connection."""
        self.connection.close()
        self.connection = None
        self.cursor = None

    def isopen(self):
        """False if connection is closed or not initialized, True if open."""
        if self.connection is None:
            return False
        try:
            self.connection.ping()
        except orcl.DatabaseError:
            return False
        except:
            raise
        else:
            return True

    # --------------------------------------------------------------------------------
    # Queries
    def query(
        self, sql: str, squeeze: bool = True, squeeze_axis: str = None
    ) -> pd.DataFrame:
        """Return query result as dataframe, squeezing it to Series/Scalar if possible (unless parameter "squeeze" is False)"""
        res = pd.read_sql_query(sql, con=self.connection)
        if squeeze:
            return res.squeeze(squeeze_axis)
        else:
            return res

    @time_log("query")
    def long_query(self, *param, **kwds):
        """Wrapper for query(), logs runtime"""
        return self.query(*param, **kwds)

    # --------------------------------------------------------------------------------
    # Procedure calls
    def exec(self, proc, *param):
        """Executes procedure proc, parameters can be supplied by position"""
        self.cursor.callproc(proc, param)

    def var(self, ora_type):
        return self.cursor.var(ora_type)

    def string_var(self):
        return self.var(orcl.STRING)

    def number_var(self):
        return self.var(orcl.NUMBER)

    def clob_var(self):
        return self.var(orcl.CLOB)

    def write_output(self, txt):
        """Write txt to DBMS_OUTPUT"""
        self.exec("dbms_output.put_line", txt)

    def iter_output(self):
        """Iterate over DBMS_OUTPUT"""
        output_line = self.string_var()
        output_status = self.number_var()
        self.exec("dbms_output.get_line", output_line, output_status)
        while output_line.getvalue() is not None:
            yield output_line.getvalue()
            self.exec("dbms_output.get_line", output_line, output_status)

    # --------------------------------------------------------------------------------
    # Private methods
    # --------------------------------------------------------------------------------
    @staticmethod
    def _connect_str(instance):
        """Build connect string for APG instance"""
        return PA_ORA_DSN_TEMPL.format(instance.lower(), instance.upper())

    def _connect(self, instance, user, passwd):
        """Build connection to APG instance. Set encoding to UTF-8, as God intended"""
        try:
            connection = orcl.connect(
                user, passwd, self._connect_str(instance), encoding="UTF-8"
            )
        except orcl.DatabaseError as d:
            err_msg = d.args[0].message
            raise orcl.DatabaseError(
                f"Connection to {user}/{passwd}@{instance} failed: {err_msg}"
            )
        except:
            raise
        else:
            return connection


###############################################################################
# TESTING CODE
###############################################################################

if __name__ == "__main__":

    # Test connectivity (all parameters supplied)
    with Connection("cheapc1", "apc", "apc_pass") as apc_test:
        if apc_test.isopen():
            print("OK: Instance connection established")
        else:
            raise RuntimeError("NOK: Instance connection not established")

    # Connect with connection tag only
    with Connection("IT21_DEV_VK") as it21_dev:
        test = it21_dev.query(
            "select 'It' HELLO, 'works!' WORLD from dual", squeeze=False
        )

    print(test)
    print(test.shape)

    # Test auto-close
    if it21_dev.isopen():
        raise RuntimeError("NOK: Instance connection still works!")
    else:
        print("OK: Instance connection closed automatically")

    # Test query
    conn = Connection("IT21_DEV_VK", do_open=True)
    print(conn.long_query("select 'User Objects: '||count(*) from user_objects"))
    print("Non-squeezed query:")
    print(conn.query("select 1 nr, 'abc' txt from dual", squeeze=False))
    print("Squeezed query (by rows):")
    print(conn.query("select 1 nr, 'abc' txt from dual", squeeze_axis="rows"))
    print("Scalar query (fully squeezed):")
    print(conn.query("select sysdate from dual"))
    conn.close()

    # Test procedure calls, DBMS_OUTPUT handling
    with Connection("cheapc1", "apc") as c:
        # low level
        o_line = c.string_var()
        o_status = c.number_var()
        c.exec("dbms_output.put_line", "Hallo Welt!")
        c.exec("dbms_output.get_line", o_line, o_status)
        print(f'Status = {o_status.getvalue()}, line = "{o_line.getvalue()}"')

        # DBMS_OUTPUT helper functions
        c.write_output("Zeile 1\nZeile 2")
        c.write_output("Zeile 3")
        c.write_output("Zeile 4\nZeile 5")
        print("\n-------\n".join(c.iter_output()))
