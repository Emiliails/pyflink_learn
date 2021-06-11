#  Copyright (c) 2021.

from pyflink.common import Row
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, StreamTableEnvironment, TableResult
# 创建 blink 批 TableEnvironment
from pyflink.table.udf import udf
from pyflink.table.expressions import col,concat

"""
参考文档
https://ci.apache.org/projects/flink/flink-docs-release-1.13/zh/docs/dev/table/tableapi/#column-operations
# 此种会出现错误
org.apache.flink.table.api.ValidationException: Querying an unbounded table 'default_catalog.default_database.random_source' in batch mode is not allowed. The table source is unbounded.

"""
# 创建 blink  TableEnvironment
env_settings_batch = EnvironmentSettings.new_instance().in_batch_mode().build()
table_env_batch = TableEnvironment.create(env_settings_batch)
table_env_batch.execute_sql("""
    CREATE TABLE random_source (
        id INT, 
        data INT ,
        name STRING
    ) WITH (
        'connector' = 'datagen',
        'rows-per-second'='1',
        'fields.id.kind'='sequence',
        'fields.id.start'='1',
        'fields.id.end'='100',
        'fields.data.min'='30',
        'fields.data.max'='200',
        'fields.name.kind'='random',
        'fields.name.length'='3'
    )
""")

table_env_batch.execute_sql('select * from random_source').print()


#
# table_env_stream.execute_sql("""
#     CREATE TABLE print_sink (
#         id INT,
#         name STRING,
#          data_sum INT
#     ) WITH (
#         'connector' = 'print'
#     )
# """)
#
# table_env_stream.execute_sql("""
#     INSERT INTO print_sink
#         SELECT id, name,sum(data) as data_sum FROM
#             (SELECT id / 2 as id,name, data FROM random_source)
#         WHERE id > 1
#         GROUP BY id,name
# """).wait()
#
# # print(table_env_stream.from_path('random_source').to_pandas())

