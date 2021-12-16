import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import * 
from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
import datetime
from awsglue import DynamicFrame

import boto3

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME", "database_name", "kinesis_table_name", "starting_position_of_kinesis_iterator", "hudi_table_name", "window_size", "s3_path_hudi", "s3_path_spark" ])

spark = SparkSession.builder.config('spark.serializer','org.apache.spark.serializer.KryoSerializer').config('spark.sql.hive.convertMetastoreParquet','false').getOrCreate()
                    
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

database_name = args["database_name"]
kinesis_table_name = args["kinesis_table_name"]
hudi_table_name = args["hudi_table_name"]
s3_path_hudi = args["s3_path_hudi"]
s3_path_spark = args["s3_path_spark"]

# can be set to "latest", "trim_horizon" or "earliest"
starting_position_of_kinesis_iterator = args["starting_position_of_kinesis_iterator"]

# The amount of time to spend processing each batch
window_size = args["window_size"]

data_frame_DataSource0 = glueContext.create_data_frame.from_catalog(
    database = database_name, 
    table_name = kinesis_table_name, 
    transformation_ctx = "DataSource0", 
    additional_options = {"inferSchema":"true","startingPosition":starting_position_of_kinesis_iterator}
)

# ensure the incomong record has the correct current schema, new fresh columns are fine, if a column exists in current schema but not in incoming record then manually add before inserting
def evolveSchema(df,table,forcecast=False):
    try:
        #get existing table's schema
        original_df = spark.sql("SELECT * FROM "+table+" LIMIT 0")
        #sanitize for hudi specific system columns
        print("\n Current schema in catalog (without hudi internal columns) is :")
        columns_to_drop = ['_hoodie_commit_time', '_hoodie_commit_seqno','_hoodie_record_key','_hoodie_partition_path','_hoodie_file_name']
        odf = original_df.drop(*columns_to_drop)
        odf.printSchema()
        if (df.schema != odf.schema):
            merged_df = df.unionByName(odf, allowMissingColumns=True)
        print("\n Schema of incoming payload from Kinesis (after merge) is : ")
        return (merged_df)
    except Exception as e:
        print (e)
        return (df)

def processBatch(data_frame, batchId):
    
    if (data_frame.count() > 0):
    
        DataSource0 = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")

        df = DataSource0.toDF()
        print("\n Schema of incoming payload from Kinesis is :")
        df.printSchema()
        
        df = evolveSchema(df,database_name +'.'+hudi_table_name ,False)

        commonConfig = {
            'path': s3_path_hudi
        }
        
        hudiWriteConfig = {
            'className' : 'org.apache.hudi',
            'hoodie.table.name': hudi_table_name,
            'hoodie.datasource.write.operation': 'upsert',
            'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
            'hoodie.datasource.write.precombine.field': 'date',
            'hoodie.datasource.write.recordkey.field': 'name',
            'hoodie.datasource.write.partitionpath.field': 'name:SIMPLE,year:SIMPLE,month:SIMPLE,day:SIMPLE',
            'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.CustomKeyGenerator',
            'hoodie.deltastreamer.keygen.timebased.timestamp.type': 'MIXED',
            'hoodie.deltastreamer.keygen.timebased.input.dateformat': 'yyyy-mm-dd',
            'hoodie.deltastreamer.keygen.timebased.output.dateformat':'yyyy/MM/dd'
        }
     
        hudiGlueConfig = {
            'hoodie.datasource.hive_sync.enable': 'true',
            'hoodie.datasource.hive_sync.sync_as_datasource': 'false',
            'hoodie.datasource.hive_sync.database': database_name,
            'hoodie.datasource.hive_sync.table': hudi_table_name,
            'hoodie.datasource.hive_sync.use_jdbc':'false',
            'hoodie.datasource.write.hive_style_partitioning' : 'true',
            'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
            'hoodie.datasource.hive_sync.partition_fields': 'name,year,month,day'
        }

        combinedConf = {
            **commonConfig, 
            **hudiWriteConfig, 
            **hudiGlueConfig
        }
        
        glueContext.write_dynamic_frame.from_options(
            frame = DynamicFrame.fromDF(df, glueContext, "df"), 
            connection_type = "custom.spark", 
            connection_options = combinedConf
        )

glueContext.forEachBatch(
    frame = data_frame_DataSource0,
    batch_function = processBatch,
    options = {
        "windowSize": window_size,
        "checkpointLocation": s3_path_spark
    }
)

job.commit()