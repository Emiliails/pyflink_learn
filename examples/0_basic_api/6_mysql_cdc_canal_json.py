"""
使用 PyFlink 进行无状态的流处理，实现 MySQL 数仓的实时同步。

拓展阅读：
https://ci.apache.org/projects/flink/flink-docs-stable/zh/dev/table/connectors/jdbc.html
https://github.com/ververica/flink-cdc-connectors
"""

import os
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, TableResult

# ########################### 初始化流处理环境 ###########################

# 创建 Blink 流处理环境
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
t_env = StreamTableEnvironment.create(environment_settings=env_settings)

# ########################### 指定 jar 依赖 ###########################
# flink-connector-jdbc_2.11-1.11.2.jar：从扩展阅读 1 里获得，作用是通过 JDBC 连接器来从数据库里读取或写入数据。
# flink-sql-connector-mysql-cdc-1.1.0.jar：从扩展阅读 2 里获得，作用是通过 MySQL-CDC 连接器从 MySQL 的 binlog 里提取更改。
# mysql-connector-java-5.1.49.jar：从扩展阅读 1 里获得，是 JDBC 连接器的驱动（ 帮助 java 连接 MySQL ）。

jars = []
for file in os.listdir(os.path.abspath(os.path.dirname(__file__))):
    if file.endswith('.jar'):
        jars.append(os.path.abspath(file))
str_jars = ';'.join(['file://' + jar for jar in jars])
#str_jars = '/Users/yikuang/BaiduPan/信息科技大学/J教学/2020-2021(2)/流数据/pyflink_learn/examples/3_database_sync/flink-sql-connector-mysql-cdc-1.4.0.jar'
t_env.get_config().get_configuration().set_string("pipeline.classpaths", str_jars)

# ########################### 创建源表(source) ###########################
# 使用 MySQL-CDC 连接器从 MySQL 的 binlog 里提取更改。
# 该连接器非官方连接器，写法请参照扩展阅读 2。

t_env.execute_sql(f"""
    CREATE TABLE source (
        id INT,              -- ID
        username STRING          -- 姓名
    ) WITH (
        'connector' = 'mysql-cdc',
        'hostname' = 'gpu',
        'port' = '9904',
        'database-name' = 'test',
        'table-name' = 'user',
        'username' = 'root',
        'password' = 'root'
    )
""")


# 查看数据方式一
# t_env.execute_sql('select * from source').print()

#
# 查看数据方式二
# result = t_env.execute_sql('select * from source ')
# with result.collect() as results:
#     for r in results:
#         print(r)


t_env.execute_sql("""
    CREATE TABLE file_sink (
        id int,
        username string
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file:///Users/yikuang/BaiduPan/信息科技大学/J教学/2020-2021(2)/流数据/pyflink_learn/examples/0_basic_api/result/',
        'format'= 'canal-json'
    )
""")
t_env.execute_sql("""
   insert into file_sink
   select id,username from source
""").wait()


