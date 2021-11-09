import snowflake.connector
from snowflake.connector import DictCursor
import traceback
import socket


class SnowflakeBase(object):
    """
    Author: Rohit Kumar
    Description: This class has defined all snowflake execution related methods like connecton object, executing sqls, procedures and etc
    """

    def __init__(self, *args, **kwargs):

        self.account = kwargs.pop("account", None)
        self.username = kwargs.pop("username", None)
        self.warehouse = kwargs.pop("warehouse", None)
        self.role = kwargs.pop("role", None)
        self.database = kwargs.pop("database", None)
        self.schema = kwargs.pop("schema", None)
        self.password = kwargs.pop("password", None)
        #self.service_account = kwargs.pop("service_account", None)

    def get_conn_params(self):
        """
        Author: Rohit Kumar
        Description
        """
        conn_config = {
            "account": self.account,
            "user": self.username,
            "database": self.database,
            "warehouse": self.warehouse,
            "role": self.role,
            "schema": self.schema,
            "password": self.password
        }

        print("Connecting to snowflake with the following details: "
              "\n\tAccount: {0}"
              "\n\tUser: {1}"
              "\n\tDatabase: {2}"
              "\n\tWarehouse: {3}"
              "\n\tRole: {4}"
              "\n\tSchema: {5}".format(self.account, self.username, self.database, self.warehouse, self.role, self.schema))

        return conn_config

    def get_conn(self):
        """
        Author: Rohit Kumar
        Description
        """

        try:
            print("Preparing Configuration")
            conn_config = self.get_conn_params()

            conn = snowflake.connector.connect(**conn_config)

            sql = "show warehouses"
            print(sql)
            conn.cursor().execute(sql)
            print("Warehouse accessible to the account: %s" % conn.cursor().execute(sql).fetchall())

            if conn_config["warehouse"]:
                if conn_config["warehouse"].strip() != "":
                    sql = "USE WAREHOUSE %s" % conn_config["warehouse"]
                    conn.cursor().execute(sql)
            if conn_config["database"]:
                if conn_config["warehouse"].strip() != "":
                    sql = "USE DATABASE %s" % conn_config["database"]
                    conn.cursor().execute(sql)
            print("connection Success")

            return conn
        except Exception as err:
            print("Failed to connect : %s" % err)
            raise

    def get_trans_conn(self):
        """
        Author: Rohit Kumar
        Description
        """

        conn_config = self.get_conn_params()
        conn = snowflake.connector.connect(**conn_config)
        conn.cursor().execute("BEGIN")
        conn.autocommit(False)
        return conn
    
    def get_trans(self):
        """
        Author: Rohit Kumar
        Description
        """

        conn_config = self.get_conn_params()
        conn = snowflake.connector.connect(**conn_config)
        # conn.cursor().execute("BEGIN")
        # conn.autocommit(False)
        return conn    

    def get_sql_with_host_name(self, querytext):
        query_with_host = "%s /*%s*/ %s" % (
        querytext.strip().split(" ", 1)[0], socket.gethostname(), querytext.strip().split(" ", 1)[1])
        return query_with_host

    def complete_trans(self, conn, isSuccess):
        if isSuccess:
            sql = "COMMIT"
        else:
            sql = "ROLLBACK"

        conn.cursor().execute(sql)
        conn.close()

    def execute_trans_sql(self, conn, querytext, target_schema=''):
        """
        Author: Rohit Kumar
        Description
        """
        print("Execute Begin : execute_sql(...)")

        try:
            if target_schema:
                if target_schema != '':
                    conn.cursor().execute("USE SCHEMA {schema}".format(schema=target_schema))

            print("Executing : %s" % self.get_sql_with_host_name(querytext))
            conn.cursor().execute(self.get_sql_with_host_name(querytext))

            print("Execution End : execute_sql(...)")
            return True
        except Exception as e:
            print("Execute Fail : execute_sql(...) - %s\n%s" % (str(e), traceback.format_exc()))

    def execute_trans_sql_raw(self, conn, querytext):
        """
        Description
        """
        print("Execute Begin : execute_trans_sql_raw(...)")

        try:
            print("Executing : %s" % self.get_sql_with_host_name(querytext))
            conn.cursor().execute(self.get_sql_with_host_name(querytext))

            print("Execution End : execute_trans_sql_raw(...)")
            return True
        except Exception as e:
            print("Execute Fail : execute_trans_sql_raw(...) - %s\n%s" % (str(e), traceback.format_exc()))
            raise e

    def execute_cursor_allrows(self, conn, querytext):
        """
        Author: Rohit Kumar
        Description
        """
        print("Execution begin : execute_cursor_allrows(...)")

        try:
            print("Executing : %s" % self.get_sql_with_host_name(querytext))
            result = conn.cursor(DictCursor).execute(self.get_sql_with_host_name(querytext)).fetchall()
            conn.close()
            print("Execution End : execute_cursor_allrows(...)")

            return result

        except Exception as e:
            print("Execution Failed : execute_cursor_allrows(...) -%s\n%s" % (str(e), traceback.format_exc()))
            raise

    def execute_sql(self, querytext, target_schema=''):
        """
        Author: Rohit Kumar
        Description
        """
        print("Execute Begin : execute_sql(...)")
        conn = None

        try:
            conn = self.get_conn()

            if target_schema:
                if target_schema != '':
                    conn.cursor().execute("USE SCHEMA {schema}".format(schema=target_schema))

            conn.cursor().execute("BEGIN")
            conn.cursor().execute(self.get_sql_with_host_name(querytext))
            conn.cursor().execute("COMMIT")
            conn.close()

            print("Execution End : execute_sql(...)")

        except Exception as e:
            if conn:
                conn.cursor().execute("ROLLBACK")
                conn.close()

            print("Execute Fail : execute_sql(...) - %s\n%s" % (str(e), traceback.format_exc()))
            raise

    def execute_cursor_firstrow_with_conn(self, querytext):
        """
        Author: Rohit Kumar
        Description
        """
        print("Execution begin : execute_cursor_firstrow_with_conn(...)")
        try:

            conn = self.get_conn()
            print("Executing : %s" % self.get_sql_with_host_name(querytext))
            result = conn.cursor(DictCursor).execute(self.get_sql_with_host_name(querytext)).fetchone()
            conn.close()
            print("Execution End : execute_cursor_firstrow_with_conn(...)")

            return result

        except Exception as e:
            print(
                "Execution Failed : execute_cursor_firstrow_with_conn(...) -%s\n%s" % (str(e), traceback.format_exc()))
            raise

    def execute_cursor_firstrow(self, conn, querytext):
        """
        Author: Rohit Kumar
        Description
        """
        print("Execution begin : execute_cursor_firstrow(...)")

        try:
            print("Executing : %s" % self.get_sql_with_host_name(querytext))
            result = conn.cursor(DictCursor).execute(self.get_sql_with_host_name(querytext)).fetchone()
            conn.close()
            print("Execution End : execute_cursor_firstrow(...)")
            return result

        except Exception as e:
            print("Execution Failed : execute_cursor_firstrow(...) -%s\n%s" % (str(e), traceback.format_exc()))
            raise

    def execute_scalar(self, querytext, select_column_name=""):
        """
        Author: Rohit Kumar
        Description
        """
        print("Execution begin : execute_scalar(...)")
        try:

            conn = self.get_conn()
            print("Executing : %s" % self.get_sql_with_host_name(querytext))
            result_record = conn.cursor(DictCursor).execute(self.get_sql_with_host_name(querytext)).fetchone()
            print(result_record)
            result_value = None

            if result_record is not None:
                if select_column_name.strip() == "":
                    select_column_name = list(result_record.keys())[0]
                result_value = result_record[select_column_name.upper()]

            conn.close()
            print("Execution End : execute_scalar(...)")

            return result_value

        except Exception as e:
            print("Execution Failed : execute_scalar(...) -%s\n%s" % (str(e), traceback.format_exc()))
            raise

    def execute_batch_scalar(self, querytext,count):
        """
        Author: Rohit Kumar
        Description
        """
        print("Execution begin : execute_scalar(...)")
        try:

            conn = self.get_conn()
            print("Executing : %s" % self.get_sql_with_host_name(querytext))
            result_record = conn.cursor(DictCursor).execute(self.get_sql_with_host_name(querytext)).fetchone()
            print(result_record)

            output = []  
            if result_record is not None:
                for i in range(0,count):
                    select_column_name = list(result_record.keys())[i]
                    output.append(result_record[select_column_name.upper()])
                    
            conn.close()
            print("Execution End : execute_scalar(...)")

            return output

        except Exception as e:
            print("Execution Failed : execute_scalar(...) -%s\n%s" % (str(e), traceback.format_exc()))
            raise
