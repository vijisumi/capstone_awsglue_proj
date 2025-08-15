import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_path ="s3://skilluprightuser13/src_capstone_proj/customer_dataset/customers_dataset.csv"

src_df=glueContext.create_dynamic_frame_from_options(connection_type="s3",
	format="csv",connection_options={"paths": [s3_path]},format_options={
	"withHeader": True,  # Set to True if your CSV has a header row
	"separator": ","     # Specify the delimiter if it's not a comma
    }
	)
df = src_df.toDF()
dedup_df=df.dropDuplicates(subset=["customer_id"])
df_clean = dedup_df.dropna(subset=['email', 'zip_code'])
cleaned_dynamic_frame = DynamicFrame.fromDF(df_clean, glueContext, "cleaned_dynamic_frame")
glueContext.write_dynamic_frame.from_options(frame=cleaned_dynamic_frame,
	connection_type="s3",
	connection_options={"path": "s3://skilluprightuser13/trg_capstone_proj/stg_layer/"},
	format="parquet",
	transformation_ctx="cleaned_data_sink"
	)
job.commit()
