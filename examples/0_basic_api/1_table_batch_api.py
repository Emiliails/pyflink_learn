#  Copyright (c) 2021.

from pyflink.common import Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, StreamTableEnvironment
# 创建 blink 批 TableEnvironment
from pyflink.table.udf import udf
from pyflink.table.expressions import col,concat

env_settings = EnvironmentSettings.new_instance().in_batch_mode().build()
table_env_batch = TableEnvironment.create(env_settings)

StreamExecutionEnvironment.get_execution_environment()
"""
参考文档
https://ci.apache.org/projects/flink/flink-docs-release-1.13/zh/docs/dev/table/tableapi/#column-operations

"""
# from elements, schema
table = table_env_batch.from_elements([(1,'M', 'Zhang','San'), (2,'M', 'Li','si'),(3,'F', 'Wang','wu')])
table.print_schema()
table = table_env_batch.from_elements([(1,'M', 'Zhang','San'), (2,'M', 'Li','si'),(3,'F', 'Wang','wu')],['id','gender','surname','givenname'])
table.print_schema()

table = table_env_batch.from_elements([(1,'M', 'Zhang','San'), (2,'M', 'Li','si'),(3,'F', 'Wang','wu')],
                                      DataTypes.ROW(
                                          [DataTypes.FIELD('id', DataTypes.INT()),
                                            DataTypes.FIELD('gender', DataTypes.STRING()),
                                            DataTypes.FIELD('surname', DataTypes.STRING()),
                                           DataTypes.FIELD('givenname', DataTypes.STRING())
                                           ]
                                      ))
table.print_schema()
# select
df = table.select(table.id, table.gender).to_pandas()
print(df)
df = table.select("id,gender").to_pandas()
print(df)
df = table.select(table.id, concat(table.surname, '-', table.givenname).alias('fullname')).to_pandas()
print(df)
df = table.select("id,surname+'-'+givenname as fullname").to_pandas()
print(df)

# filter
df = table.filter('id==1').to_pandas()
print(df)
df = table.where('id==1').to_pandas()
print(df)
pandas = table.where(table.id == 1).to_pandas()
print(pandas)
df = table.filter('id==1').select(table.id, table.gender).to_pandas()
print(df)
df = table.filter('id==1').select(table.id, concat(table.gender, '-', table.surname)).to_pandas()
print(df)
df = table.filter('id==1').select("id,gender").to_pandas()
print(df)

# group by
df = table.group_by(table.gender).select(table.gender,table.surname.count.alias('cnt')).to_pandas()
df = table.group_by(table.gender).select("gender,count(surname) as cnt").to_pandas()
print(df.head())

# distinct
df = table.select('gender').distinct().to_pandas()
print(df)

# columns
df =table.add_columns(concat(table.surname,'-',table.givenname).alias('fullname')).to_pandas()
print(df)
df = table.drop_columns('gender').to_pandas()
print(df)
print(table.to_pandas())

df = table.rename_columns("gender as g").to_pandas()
print(df)
print(table.to_pandas())


#offset orderby
df = table.order_by(table.id.desc).to_pandas()

df = table.offset(1).to_pandas()
print(df)
df = table.offset(1).fetch(1).to_pandas()

# join

table_score = table_env_batch.from_elements([(1,80), (2,90),(3,70)],['seq','score'])
df =table.join(table_score,table.id == table_score.seq).to_pandas()

# row based operations
df = table.map(udf(lambda x: Row(x[0] + 1, x[1] + 'yy'), result_type=DataTypes.ROW(
    [DataTypes.FIELD('id', DataTypes.BIGINT()), DataTypes.FIELD('name', DataTypes.STRING())]))).to_pandas()
print(df)

#SQL API
table_env_batch.create_temporary_view('temp_view',table)
table_env_batch.execute_sql('select * from temp_view').print()
table_env_batch.execute_sql('select gender,count(id) from temp_view group by gender').print()
