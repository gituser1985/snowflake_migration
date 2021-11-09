from .snowflake_base import SnowflakeBase



class SnowflakeHook(SnowflakeBase):
    """
    Author: Rohit Kumar
    Description
    """

    def __init__(self, *args, **kwargs):
        super(SnowflakeHook, self).__init__(*args, **kwargs)


    def get_copy_stmt(self, target_database, target_table, target_schema, source_system_name, time_zone, created_by,
                      updated_by,
                      inprogress_location, snowflake_file_format):
        print("Execution begin : get_copy_stmt(...)")

        column_count = self.get_column_count(target_database, target_table, target_schema)

        target_table_name = "{db}.{schema}.{table}".format(db=target_database,
                                                           schema=target_schema,
                                                           table=target_table)

        select_stmt = self.get_col_ordinal_position(column_count)

        select_stmt += "'{source_system_name}', '{time_zone}', {created_date}, '{created_by}', {updated_date}, " \
                       "'{updated_by}'".format(
            source_system_name=source_system_name,
            time_zone=time_zone,
            created_date='current_timestamp()',
            created_by=created_by,
            updated_date='current_timestamp()',
            updated_by=updated_by
        )

        copy_sql_stmt = "COPY INTO {tgt_tbl} from (" \
                        "select {sel_stmt} from @{db}.DAAS_COMMON.DAAS_EXT_STG/{stg_loc} (file_format => {db}.DAAS_COMMON.{" \
                        "file_format}));".format(
            tgt_tbl=target_table_name,
            sel_stmt=select_stmt,
            stg_loc=inprogress_location,
            file_format=snowflake_file_format,
            db=target_database,
            schema=target_schema)

        print(copy_sql_stmt)
        print("Execution End : get_copy_stmt(...)")

        return copy_sql_stmt


    def get_stage_count(self, target_database, target_schema, target_table, batch_id, 
                                             column_name):

        print("Execution Begin: get_stage_count(..)")

        sql_stmt = "select count(*) as {column_name} from {target_schema}.{target_table} " \
                   "where BATCH_ID = '{batch_id}';".format(target_schema=target_schema,
                                                           target_table=target_table,
                                                           batch_id=batch_id,
                                                           column_name=column_name)

        print(sql_stmt)
        print("Execution End : get_last_extract_tmst_from_cntrl_tbl(...)")
        return sql_stmt
