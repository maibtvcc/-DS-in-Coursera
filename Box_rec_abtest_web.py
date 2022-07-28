#!/usr/bin/env python
# coding: utf-8

# this task is used to ELT data and push results to data warehouse

import numpy as np
import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import Window
import os
import re
import pymysql.cursors
import json
import aerospike
import sys
import requests
from pyspark.sql.types import *

res = requests.get(
    "")

file = open("helpers.py", "wb")
file.write(res.content)
file.close()

exec(open("helpers.py").read())
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

domain = sys.argv[1]

spark=SparkSessionCreator(app_name=f"{domain}box_evaluation",   executor_memory='8G', driver_memory='8G', cores=2).create_spark_session(include_mySQL=True)

with open("/home/airflow/datamining/config/config_da.json") as json_data_file:
    cfg = json.load(json_data_file)
acc_cfg = cfg['airflow_bi']
user = acc_cfg['user']
password = acc_cfg['password']

d=datetime.now() - timedelta(days=1)

if domain in (['kenh14']):
    config = {
        'hosts': #server and port
        }
    client = aerospike.client(config).connect()

    keys=[]
    scan = client.scan('hdd_hadoop','ab-testing-log')
    scan_opts = { 'concurrent': True, 'nobins': True, 'priority': aerospike.SCAN_PRIORITY_MEDIUM }
    for x in (scan.results(policy=scan_opts)): keys.append([x[0][2],x[2].get('bucket')])
    client.close()

    df_user = pd.DataFrame(data=keys)
    df_user = df_user[~df_user[1].isnull()]
    df_user=df_user[~df_user[0].isnull()]
    df_user = df_user.astype('string')

    df_user['keygen']=df_user[0].apply(lambda x: x.split(":")[0])
    df_user['guid']=df_user[0].apply(lambda x: x.split(":")[1])
    df_user['group_ab'] = np.where(df_user[1]=='alg1','a',
                                np.where(df_user[1]=='alg2','b',
                                        np.where(df_user[1]=='alg3','c',"")))
    df_user= df_user[df_user.group_ab!='']
    

# get running box_test 
def get_mysql(query):
    connection = pymysql.connect(host='',
                                     user=user,
                                    password=password,
                                     database='bi_data_analyst',
                                     cursorclass=pymysql.cursors.DictCursor)


    dbCursor = connection.cursor()
    dbCursor.execute(query)
    result = dbCursor.fetchall()
    connection.commit()
    connection.close()
    data = pd.DataFrame(result)
    data = data.astype('str')
    return data

df_Abtest = get_mysql(f"""SELECT * FROM recengine_abtest_info WHERE period like 'On%' and domain = '{domain}'""")


# delete duplicate before appending to the tables
get_mysql(f"""DELETE FROM recengine_abtest_web where date(date) = '{str(d.date())}' and domain = '{domain}'""")   
get_mysql(f"""DELETE FROM recengine_abtest_useractive where date(date) = '{str(d.date())}' and domain = '{domain}'""")         


#lấy box id từ ls box_id-itemId
def extract_box_id(x):
    ''' lấy tên box từ itemsBox '''
    x= x[:x.find('reqid')-1]
    ls_id=x.split('-')
    box_id=''
    for string in ls_id:
        if (bool(re.search(r'[a-zA-Z]',string))==True):
            box_id=box_id+"-"+string
    box_id = box_id[1:]
    return box_id
extract_box_id_udf=F.udf(lambda x: extract_box_id(x))

#lấy itemId
def extract_item(x):
    return x.split('-')[-1]

extract_item_udf = F.udf(lambda x: extract_item(x))


# get all log of domain
def get_all_log(domain):
     #log tl: chứa data user tương tác với các box trên web
    path ='hdfs://'
    base_filter=((F.col('domain')==f'{domain}.vn') & (F.col('guid')!='-1')
                 & (F.col('itemsBox').isNotNull()))
    helper = Helper(sparkss=spark, path=path, base_filter=base_filter)
    selects=['dt','guid', 'itemsBox', 'clickOrView', 'pageLoadId']
    df_box=helper.get_single_spark_df(d.date(), selects=selects)
    user_active = df_box.select('guid').dropDuplicates().count()
    # get session:
    df_box = df_box.withColumn('dt',unix_timestamp(to_timestamp('dt')))

    df_box = df_box.sort('guid','dt')

    from pyspark.sql.window import Window

    #chia session

    window = Window.partitionBy('guid').orderBy('dt')

    df_box = df_box.withColumn("seconds_diff", - df_box.dt + F.lead(df_box.dt, 1).over(window)).cache()

    #xác định điểm cuối của 1 session
    df_box=df_box.withColumn('lastBox', F.when(( (df_box.seconds_diff >=1200) | (df_box.seconds_diff.isNull()) ), 1).otherwise(0))

    #assign session_id cho mỗi session
    window = Window.orderBy('guid', 'dt')
    df_box = df_box.withColumn('session', F.sum(df_box.lastBox).over(window)+ when(col('lastBox')==1,0).otherwise(1))
    df_box = df_box.drop('seconds_diff')
    
    #cột itemsBox có thể chứa nhiều box_id cùng lúc nên phải split
    df_box=df_box.withColumn('ls_item_box', F.split(F.col('itemsBox'),","))   
    return df_box,user_active

config = {
      'hosts': [ ('10.3.50.138', 11000),('10.3.48.161', 11000),('10.3.51.160', 11000),('10.3.48.248', 11000) ]
    }
client = aerospike.client(config).connect()

def get_df_all(domain,box_test):
    #scan list user từ aero
    keygen = df_Abtest[(df_Abtest.domain==domain)&(df_Abtest.box_test == box_test)].keygen.iloc[0]
    if keygen=='None':
        key = ('mem_storage', f'recengine_abtest_guid_{domain}', box_test)
        (key, meta, list_user) = client.get(key)

        df_all_users = pd.DataFrame(columns = ['guid', 'group_ab'])
        for key in list_user.keys():
            data=pd.DataFrame(list_user[key]).rename(columns={0:'guid'})
            data['group_ab'] = key
            df_all_users=pd.concat([df_all_users, data], ignore_index=True)
    else:
        df_all_users = df_user[df_user.keygen == keygen]

    df_users=spark.createDataFrame(df_all_users[['guid', 'group_ab']])
    df_box=df.join(df_users,on='guid', how='inner') 
    user_active = df_box.groupby('group_ab').agg(countDistinct('guid').alias('user_active'))
        #format lại để mỗi row là 1 box_id
    df_box=df_box.select('dt','guid', 'itemsBox', 'clickOrView', 'pageLoadId','session','lastBox','group_ab', F.explode(df_box.ls_item_box).alias("ls_item_box"))
    df_box=df_box.withColumn('box', extract_box_id_udf(F.col('ls_item_box')))
    df_box=df_box.withColumn('itemId', extract_item_udf(F.col('ls_item_box')))
    df_box=df_box.dropDuplicates()

    df_click=df_box[df_box.clickOrView==1].groupBy(['box', 'group_ab']).agg(F.count('itemsBox').alias('total_click'),
                                                                                 countDistinct('guid').alias('total_user_click'))
    df_view=df_box[df_box.clickOrView==0].groupBy(['box', 'group_ab']).agg(F.count('itemsBox').alias('total_view'),
                                                                                countDistinct('guid').alias('total_user_view'))
    
    dfall = df_click.join(df_view, on=['box', 'group_ab'], how='outer').fillna(0)


    # Tỷ lệ view đến và thoát khi view đến box
    columns = StructType([StructField('box',StringType(), True),StructField('group_ab',StringType(), True),
                          StructField('exit_rate',FloatType(), True),StructField('view_rate',FloatType(), True)])

    df_sesBox = spark.createDataFrame(data = [],schema = columns)

    list_user = df_all_users.group_ab.drop_duplicates().tolist()
    for group in list_user:
        sesNum = df_box.filter(df_box.group_ab == group).dropDuplicates(subset = ['session']).count()
        df_sesBox1 = df_box.filter(df_box.group_ab == group).groupBy('box','group_ab').agg(
            countDistinct(when(col('lastBox')==1,col('session'))).alias('exit_rate'),
            countDistinct(col('session')).alias('view_rate'))
        df_sesBox1 = df_sesBox1.withColumn('exit_rate',round(col('exit_rate')/col('view_rate'),4))\
                            .withColumn('view_rate',round(col('view_rate')/sesNum,4))
        df_sesBox = df_sesBox.union(df_sesBox1)

    dfall = dfall.join(df_sesBox, on = ['box','group_ab'], how = 'left')
    dfall = dfall.withColumn('block_view',lit(None))
    # block_view
    df1 = df_box.dropDuplicates(['dt','guid','itemsBox','box'])
    df_click1=df1[df1.clickOrView==1].groupBy(['box', 'group_ab']).agg(F.count('itemsBox').alias('total_click'),
                                                                             countDistinct('guid').alias('total_user_click'))
    df_view1=df1[df1.clickOrView==0].groupBy(['box', 'group_ab']).agg(F.count('itemsBox').alias('total_view'),
                                                                                countDistinct('guid').alias('total_user_view'))

    dfall1 = df_click1.join(df_view1, on=['box', 'group_ab'], how='outer').fillna(0)
    dfall1 = dfall1.withColumn('exit_rate',lit(None)).withColumn('view_rate',lit(None))
    dfall = dfall.unionByName(dfall1.withColumn('block_view',lit('x')))
    
    dfall = (dfall.withColumn('domain', F.lit(domain)).withColumn('box_test',F.lit(box_test)).withColumnRenamed('box', 'box_id'))
    return dfall, user_active
    

columns = StructType([StructField('box_id',StringType(), True),StructField('group_ab',StringType(), True),
                      StructField('total_click',IntegerType(), True),StructField('total_user_click',IntegerType(), True),
                      StructField('total_view',IntegerType(), True),StructField('total_user_view',IntegerType(), True),
                      StructField('exit_rate',FloatType(), True),StructField('view_rate',FloatType(), True),
                      StructField('block_view',StringType(), True),
                      StructField('domain',StringType(), True),StructField('box_test',StringType(), True)])

listBoxtest =  df_Abtest['box_test'].drop_duplicates().tolist()
df,user_active_site = get_all_log(domain)
dfall = spark.createDataFrame(data = [],schema = columns)

for box_test in listBoxtest:
    df1,user_active = get_df_all(domain,box_test)
    dfall = dfall.unionByName(df1)
    user_active = user_active.withColumn('box_test',lit(box_test)).withColumn('domain',lit(domain))\
                    .withColumn('user_active_site',lit(user_active_site)).withColumn('date',lit(d.date()))
    user_active.write.format('jdbc').options(url='jdbc:mysql://...?useSSL=false',
                                    driver='com.mysql.jdbc.Driver',
                                    dbtable='recengine_abtest_useractive',
                                    user=user,
                                    password=password,
                                    characterEncoding='utf8',   
                                    useUnicode=True).mode('append').save()
dfall = dfall.withColumn('date',lit(d.date()))
# append to db
dfall.write.format('jdbc').options(url='jdbc:mysql://...?useSSL=false',
                                    driver='com.mysql.jdbc.Driver',
                                    dbtable='recengine_abtest_web',
                                    user=user,
                                    password=password,
                                    characterEncoding='utf8',
                                    useUnicode=True).mode('append').save()


client.close()
spark.stop()

