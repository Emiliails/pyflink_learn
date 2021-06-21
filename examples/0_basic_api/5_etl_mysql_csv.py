#  Copyright (c) 2021.
import os

from pyflink.common import Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, StreamTableEnvironment, TableResult
# 创建 blink 批 TableEnvironment
from pyflink.table.udf import udf
from pyflink.table.expressions import col,concat

"""
参考文档
https://ci.apache.org/projects/flink/flink-docs-release-1.13/zh/docs/dev/table/tableapi/#column-operations

"""
# 创建 blink  TableEnvironment
env_settings_stream = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env_stream = TableEnvironment.create(env_settings_stream)


jars = []
for file in os.listdir(os.path.abspath(os.path.dirname(__file__))):
    if file.endswith('.jar'):
        jars.append(os.path.abspath(file))
str_jars = ';'.join(['file://' + jar for jar in jars])
#str_jars = '/Users/yikuang/BaiduPan/信息科技大学/J教学/2020-2021(2)/流数据/pyflink_learn/examples/3_database_sync/flink-sql-connector-mysql-cdc-1.4.0.jar'
table_env_stream.get_config().get_configuration().set_string("pipeline.classpaths", str_jars)

#
# 处理时间和事件时间的对比
# # 基于 SQL API
table_env_stream.execute_sql(f"""
    CREATE TABLE sink (
        id INT,                            -- ID
        username STRING,                       -- 姓名
        PRIMARY KEY (id) NOT ENFORCED      -- 需要定义主键，让连接器在 upsert 模式下运行
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:mysql://127.0.0.1:9904/test',
        'driver' = 'com.mysql.cj.jdbc.Driver',
        'table-name' = 'sink',
        'username' = 'root',
        'password' = 'root'
    )
""")


# 转换处理，流式输出到文件

table_env_stream.execute_sql("""
    CREATE TABLE file_sink (
        id int,
        username string
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file:///Users/yikuang/BaiduPan/信息科技大学/J教学/2020-2021(2)/流数据/pyflink_learn/examples/0_basic_api/result/',
        'format'= 'csv'
    )
""")
table_env_stream.execute_sql("""
   insert into file_sink
   select id,username from sink
""").wait()
#

# # 筛选后写入raw文件
# # table_env_stream.execute_sql("""
# #     INSERT INTO file_sink
# #         SELECT cast(id  as string) || ',' || cast(data as string) || ',' ||name FROM random_source
# #         where id > 20
# # """).wait()
#
# # 筛选后写入CSV
# table_env_stream.execute_sql("""
#     INSERT INTO file_sink
#         SELECT id, data/2,name FROM random_source
#         where id > 20
# """).wait()

# 输出到print connector
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
#         SELECT id,sum(data) as data_sum FROM
#             (SELECT id / 2 as id,name, data FROM random_source)
#         WHERE id > 1
#         GROUP BY id,name
# """).wait()
#
# # print(table_env_stream.from_path('random_source').to_pandas())

