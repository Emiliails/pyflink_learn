"""
使用 PyFlink 进行批处理。

网上可查的教程往往基于 Table API 来实现 word count。
本教程同时还提供了基于更 high-level 的 SQL API 的实践。
"""

import os
import shutil
from pyflink.table import  EnvironmentSettings, TableEnvironment
from pyflink.table import DataTypes
from pyflink.table.descriptors import Schema, OldCsv, FileSystem

# ########################### 初始化环境 ###########################

# 创建环境
env_settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
t_env = TableEnvironment.create(environment_settings=env_settings)


# ########################### 创建源表(source) ###########################
# source 指数据源，即待处理的数据流的源头，这里使用同级目录下的 word.csv，实际中可能来自于 MySQL、Kafka、Hive 等

dir_word = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'word.csv')

# 基于 SQL API
t_env.execute_sql(f"""
    CREATE TABLE source (
        id BIGINT,     -- ID
        word STRING    -- 单词
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file://{dir_word}',
        'format' = 'csv'
    )
""")

# 基于 Table API
# t_env.connect(FileSystem().path(dir_word)) \
#     .with_format(OldCsv()
#                  .field('id', DataTypes.BIGINT())
#                  .field('word', DataTypes.STRING())) \
#     .with_schema(Schema()
#                  .field('id', DataTypes.BIGINT())
#                  .field('word', DataTypes.STRING())) \
#     .create_temporary_table('source')

# 查看数据
# t_env.from_path('source').print_schema()  # 查看 schema
# t_env.from_path('source').to_pandas()  # 转为 pandas.DataFrame

# ########################### 创建结果表(sink) ###########################
# sink 指发送数据，即待处理的数据流的出口，这里使用同级目录下的 result.csv，实际中可能会把处理好的数据存到 MySQL、Kafka、Hive 等

dir_result = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'result')

# 如果文件/文件夹存在，则删除
if os.path.exists(dir_result):
    if os.path.isfile(dir_result):
        os.remove(dir_result)
    else:
        shutil.rmtree(dir_result, True)

# 基于 SQL API
t_env.execute_sql(f"""
    CREATE TABLE sink (
        word STRING,   -- 单词
        cnt BIGINT     -- 出现次数
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file://{dir_result}',
        'format' = 'csv'
    )
""")

t_env.execute_sql(f"""
    CREATE TABLE sink_print (
        word STRING,   -- 单词
        cnt BIGINT     -- 出现次数
    ) WITH (
        'connector' = 'print'
    )
""")
# 基于 SQL API
# t_env.connect(FileSystem().path(dir_result)) \
#     .with_format(OldCsv()
#                  .field('word', DataTypes.STRING())
#                  .field('cnt', DataTypes.BIGINT())) \
#     .with_schema(Schema()
#                  .field('word', DataTypes.STRING())
#                  .field('cnt', DataTypes.BIGINT())) \
#     .create_temporary_table('sink')

# ########################### 批处理任务 ###########################

# 基于 SQL API
t_env.sql_query("""
    SELECT word
           , count(1) AS cnt
    FROM source
    GROUP BY word
""").execute_insert('sink_print').wait()

t_env.execute_sql('insert into sink_print select word,count(1) as cnt from source group by word order by cnt').wait()
# 基于 table API
# t_env.from_path('source') \
#     .group_by('word') \
#     .select('word, count(1) AS cnt') \
#     .execute_insert('sink')
