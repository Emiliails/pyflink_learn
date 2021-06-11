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
# 事件时间的对比
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



# 流式查询数据
table_env_stream.execute_sql('select * from random_source ').print()

# 流式窗口查询
"""
# 参考资料
# https://ci.apache.org/projects/flink/flink-docs-release-1.13/zh/docs/dev/table/sql/queries/window-tvf/
# """

table_env_stream.execute_sql('''
select TUMBLE_START(ts,interval '5' seconds),TUMBLE_END(ts,interval '5' seconds),avg(age) ,count(id)
from random_source
group by tumble(ts ,interval '5' seconds )
''').print()

#处理时间与事件时间的对比
# ts  AS PROCTIME()
table_env_stream.execute_sql("""
    CREATE TABLE random_source_proc (
        id INT, 
        age INT ,
        name STRING,
        ts as PROCTIME()
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
table_env_stream.execute_sql('''
select TUMBLE_START(ts,interval '5' seconds),TUMBLE_END(ts,interval '5' seconds),avg(age) ,count(id)
from random_source_proc
group by tumble(ts ,interval '5' seconds )
''').print()