import sys
from awsglue.transforms import *
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import logging

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
s3_path ="s3://skilluprightuser13/trg_capstone_proj/stg_layer/"
logger = logging.getLogger(__name__) 
logger.setLevel(logging.INFO) 
logger.info("Read Customer data")
cust_data_df=glueContext.create_dynamic_frame.from_options(connection_type="s3",
	format="parquet",connection_options={"paths": [s3_path]},
	transformation_ctx="cust_data_df"
	)
logger.info("Read transacation data")
trans_data_df=glueContext.create_dynamic_frame_from_options(connection_type="s3",
	format="csv",connection_options={"paths": ["s3://skilluprightuser13/src_capstone_proj/transaction_data/transactions_dataset.csv"]},format_options={
	"withHeader": True,  # Set to True if your CSV has a header row
	"separator": ","     # Specify the delimiter if it's not a comma
    },transformation_ctx="trans_data_df"
	)
logger.info("Read Geolocation data")
geo_data_df=glueContext.create_dynamic_frame.from_options(connection_type="s3",
	format="json",connection_options={"paths": ["s3://skilluprightuser13/src_capstone_proj/geolocation_dataset/geolocation_dataset.json"]}
	)
cust_df = cust_data_df.toDF()
trans_df = trans_data_df.toDF()
geo_df = geo_data_df.toDF()
logger.info("Join Customer and geo  data")
cust_geo_df=cust_df.join(geo_df,cust_df["zip_code"]==geo_df["zip_code"],"inner")
cust_geo_df1=cust_geo_df.select(cust_df["*"],geo_df["city"],geo_df["state"])
cust_geo_trans_df=cust_geo_df1.join(trans_df,cust_geo_df1["customer_id"]==trans_df["customer_id"],"inner")
cust_geo_trans=cust_geo_trans_df.select(cust_geo_df1["*"],trans_df["transaction_amount"],trans_df["transaction_date"])
logger.info("Derive transaction total amount and transaction amount")
total_amount=cust_geo_trans.groupby("customer_id").agg(F.sum("transaction_amount").alias("total_transaction_amount"))
total_count=cust_geo_trans.groupby("customer_id").count().alias("transaction_count")
derived_df=total_amount.join(total_count,total_amount["customer_id"]==total_count["customer_id"],"inner").select(total_amount["*"],total_count["count"].alias("transaction_count"))
logger.info("Derive final dataframe data")
derive_fn_df=cust_geo_df1.join(derived_df,cust_geo_df1["customer_id"]==derived_df["customer_id"],"inner").select(cust_geo_df1["*"],derived_df["total_transaction_amount"],derived_df["transaction_count"])
derive_fn_df1=derive_fn_df.withColumn("Year",F.year("created_at"))

cleaned_dynamic_frame = DynamicFrame.fromDF(derive_fn_df1, glueContext, "cleaned_dynamic_frame")
glueContext.write_dynamic_frame.from_options(frame=cleaned_dynamic_frame,
	connection_type="s3",
	connection_options={"path": "s3://skilluprightuser13/trg_capstone_proj/final_tgt_data/", "partitionKeys": ["state","Year"]},
	format="parquet",
	transformation_ctx="cleaned_data_sink"
	)
job.commit()
