#  Copyright (c) 2021.

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
#
# 处理时间和事件时间的对比
#ts  AS PROCTIME()
table_env_stream.execute_sql("""
    CREATE TABLE random_source (
        id INT, 
        age INT ,
        name STRING,
        ts as localtimestamp,
 watermark for ts AS ts - INTERVAL '2' SECOND
        
    ) WITH (
        'connector' = 'datagen',
        'rows-per-second'='1',
        'fields.id.kind'='sequence',
        'fields.id.start'='1',
        'fields.id.end'='10',
        'fields.age.min'='1',
        'fields.age.max' = '20',
        'fields.name.length'='1'
    )
""")


# 转换处理，流式输出到文件

table_env_stream.execute_sql("""
    CREATE TABLE file_sink (
        id int,
        age int,
        name string,
        ts timestamp(3),
         watermark for ts AS ts - INTERVAL '2' SECOND

    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file:///Users/yikuang/BaiduPan/信息科技大学/J教学/2020-2021(2)/流数据/pyflink_learn/examples/0_basic_api/result/',
        'format'= 'csv'
    )
""")
table_env_stream.execute_sql("""
 insert into file_sink(id,age,name,ts) 
     select id,age,name,ts from random_source
""").wait()

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


# 流式查询数据
table_env_stream.execute_sql('select * from file_sink ').print()

# table_env_stream.execute_sql('select name ,sum(data),count(data) from random_source group by name ').print()

# 流式窗口查询
"""
# 参考资料
# https://ci.apache.org/projects/flink/flink-docs-release-1.13/zh/docs/dev/table/sql/queries/window-tvf/
# """
table_env_stream.execute_sql('''
select TUMBLE_START(ts,interval '5' seconds),TUMBLE_END(ts,interval '5' seconds),avg(age) ,count(id)
from file_sink
group by tumble(ts ,interval '5' seconds )
''').print()
table_env_stream.execute_sql('''
select TUMBLE_START(ts,interval '5' seconds),TUMBLE_END(ts,interval '5' seconds),avg(age) ,count(id)
from random_source
group by tumble(ts ,interval '5' seconds )
''').print()