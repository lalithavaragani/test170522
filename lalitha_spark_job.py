# Importing required packages
from pyspark.sql.types import DecimalType,StringType
from pyspark.sql import functions as f
import json
import sys
from pyspark.sql import SparkSession 
from pyspark.conf import SparkConf
import pyspark.sql.utils 
from pyspark.sql.utils import AnalysisException

# Initialising Spark 
spark = SparkSession.builder.appName('Data_Transformation').getOrCreate()

# Loading Delta from Jar file in S3
delta_jar = sys.argv[2]
spark.sparkContext.addPyFile(delta_jar)

# Importing delta
from delta import *

class Data_Transformation():
    
    # The __init__ function is called every time an object is created from a class
    def __init__(self):
    
        # reading app-config file
        self.jsonData=self.read_config(app_config)
        
        # Assigning variables for app_configuration File Parameters        
        self.ingest_datasets = self.jsonData['datasets']
        self.ingest_actives_source = self.jsonData['ingest-Actives']['source']['data-location']
        self.ingest_actives_destination = self.jsonData['ingest-Actives']['destination']['data-location']
        self.ingest_viewership_source = self.jsonData['ingest-Viewership']['source']['data-location']
        self.ingest_viewership_destination = self.jsonData['ingest-Viewership']['destination']['data-location']
        self.transformation_cols_actives = self.jsonData['masked-Actives']['transformation-cols']
        self.transformation_cols_viewership = self.jsonData['masked-Viewership']['transformation-cols']
        self.ingest_raw_actives_source = self.jsonData['masked-Actives']['source']['data-location']
        self.ingest_raw_viewership_source = self.jsonData['masked-Viewership']['source']['data-location']
        self.ingest_raw_actives_Destination = self.jsonData['masked-Actives']['destination']['data-location']
        self.ingest_raw_viewership_Destination = self.jsonData['masked-Viewership']['destination']['data-location']
        self.masking_col_actives= self.jsonData['masked-Actives']['masking-cols']
        self.masking_col_viewership= self.jsonData['masked-Viewership']['masking-cols']        
        self.partition_col_actives= self.jsonData['masked-Actives']['partition-cols']
        self.partition_col_viewership= self.jsonData['masked-Viewership']['partition-cols']
        self.lookup_location = self.jsonData['lookup-dataset']['data-location']
        self.pii_cols =  self.jsonData['lookup-dataset']['pii-cols']
        
        
                      
    #Function to read app_configuration file
    def read_config(self,app_config):
        configData = spark.sparkContext.textFile(app_config).collect()
        data       = ''.join(configData)
        jsonData = json.loads(data)
        return jsonData
        
    #Function to read file from s3 Location
    def read_data(self, path):
        df = spark.read.parquet(path)
        return df
    
    #Function to write file to s3 Location
    def write_data(self, df, path, partition_cols = []):
        if partition_cols:
            df.write.mode("overwrite").partitionBy(partition_cols[0], partition_cols[1]).parquet(path)
        else:
            df.write.mode("overwrite").parquet(path)
        
    #Function to mask PII columns in a file
    def mask_data(self, df, column_list):
        for column in column_list:
            #df = df.withColumn("masked_"+column,f.concat(f.lit('***'),f.substring(f.col(column),4,3)))
            df = df.withColumn("masked_"+column,f.sha2(f.col(column),256))
        return df
    
    #Function to casting required fileds in a file
    def transformation(self, df, cast_dict):
        for key in cast_dict.keys(): 
            if cast_dict[key].split(",")[0] == "DecimalType":
                df = df.withColumn(key, df[key].cast(DecimalType(10, int(cast_dict[key].split(",")[1])))) 
            elif cast_dict[key] == "ArrayType-StringType":
                df.withColumn(key,f.concat_ws(",",f.col(key)))
        return df
      
    def scd2_implementaion(self, df, lookup_location, pii_cols):        
        file_name = "Actives"
        # Adding start_date and end_date columns to df
        df_source = df.withColumn("start_date",f.current_date())
        df_source = df_source.withColumn("end_date",f.lit("null"))
        
          
        # getting required (masked and unmasked pii columns) from a dataset for delta table
        source_columns = []
        for col in pii_cols:
            if col in df.columns:
                source_columns += [col,"masked_"+col]
        
        # adding start_date, end_date columns to delta table creation
        source_columns_used = source_columns + ['start_date','end_date']        
        df_source = df_source.select(*source_columns_used)
        
        # Checking if Delta Table exists 
        try:
            # Retrieving Delta Table if table already exists
            targetTable = DeltaTable.forPath(spark, lookup_location + file_name)
            # Converting Delta Table to DF
            delta_df = targetTable.toDF()
        except pyspark.sql.utils.AnalysisException:
            print('Table does not exist')
            # Creating delta table if not exists
            df_source = df_source.withColumn("flag_active",f.lit("true"))
            df_source.write.format("delta").mode("overwrite").save(lookup_location + file_name)
            print('Table Created Sucessfully!')
            # Retrieving Delta Table
            targetTable = DeltaTable.forPath(spark, lookup_location + file_name)
            # Converting Delta Table to DF
            delta_df = targetTable.toDF()
            delta_df.show(10)
            
        # updating delta_df columns names  with target_+ columnname    
        delta_df = delta_df.select(*(f.col(i).alias('target_' + i) for i in delta_df.columns))
        # Joining condition 
        join_condition = (df_source.advertising_id == delta_df.target_advertising_id) | (df_source.user_id == delta_df.target_user_id)
        # performing leftouter join with df_source and delta_df 
        join_df = df_source.join(delta_df, join_condition, "leftouter").select(df_source['*'], delta_df['*'])
        
        # filtering new records for insert
        new_data = join_df.filter("target_user_id is null")
        # filtering new records for update
        filter_df = join_df.filter(join_df.user_id != join_df.target_user_id)
        # combining both insert and update data
        filter_df = filter_df.union(new_data)
        
        if filter_df.count() != 0:            
            mergeDf = filter_df.withColumn("MERGEKEY", f.concat(filter_df.advertising_id, filter_df.target_user_id))
            tempDf = filter_df.filter("target_advertising_id is not null").withColumn("MERGEKEY", f.lit(None))

            scdDF = mergeDf.union(tempDf)
            
            delta_columns = [i for i in delta_df.columns if i not in ['start_date', 'end_date', 'flag_active']]
            
            # creating dictionary to add new record details
            insert_dict = {}
            for i in delta_columns:
                insert_dict[i] = "updates."+i
            # Adding flag_active, start_date, end_date columns for new records dictionary 
            insert_dict['begin_date'] = f.current_date()
            insert_dict['flag_active'] = "True" 
            insert_dict['update_date'] = "null"
                               
            # Appling SCD Type 2 Operation using Merge
            targetTable.alias("target").merge(
                source=scdDF.alias("source"),
                condition="concat(target.advertising_id, target.user_id) = source.MERGEKEY and target.flag_active = 'true'"
            ).whenMatchedUpdate(set={
                # Set flag_active as false and end_date as new record start date.
                "end_date": "current_date",
                "flag_active": "False",
            }).whenNotMatchedInsert(values=insert_dict
            ).execute()
            
        for i in pii_cols:
            df = df.drop(i).withColumnRenamed("masked_"+i, i)

        return df
		
		
     

# reading app_config path from a variable
app_config= sys.argv[1]
# Creating an object for Data_Transformation class
T = Data_Transformation()

for dataset in T.ingest_datasets:
    if dataset == "Actives": 
        # read actives files from Landing Zone
        actives_landing_data = T.read_data(T.ingest_actives_source)
        # write actives files from Landing Zone to Raw Zone
        T.write_data(actives_landing_data, T.ingest_actives_destination)
        # read actives files from Raw Zone
        actives_raw_data = T.read_data(T.ingest_raw_actives_source)
        # masking some fields from actives files
        actives_masked_data = T.mask_data(actives_raw_data, T.masking_col_actives)
        # casting some fields from actives files
        actives_tranform_data = T.transformation(actives_masked_data, T.transformation_cols_actives)
        
        #scd2 implementation
        lookup_data = T.scd2_implementaion(actives_tranform_data, T.lookup_location, T.pii_cols)
        # write actives Transformed data to Staging Zone
        T.write_data(lookup_data, T.ingest_raw_actives_Destination, T.partition_col_actives)
        
    elif dataset == "Viewership":
        # read viwership files from Landing Zone
        viewership_landing_data = T.read_data(T.ingest_viewership_source) 
        # write viwership files from Landing Zone to Raw Zone
        T.write_data(viewership_landing_data, T.ingest_viewership_destination) 
        # read viwership files from Raw Zone
        viewership_raw_data = T.read_data(T.ingest_raw_viewership_source)
        # masking some fields from viwership files
        viewership_masked_data = T.mask_data(viewership_raw_data, T.masking_col_viewership)   
        # casting some fields from viwership files
        viewership_transform_data = T.transformation(viewership_masked_data, T.transformation_cols_viewership) 
        # write viwership Transformed data to Staging Zone
        T.write_data(viewership_transform_data, T.ingest_raw_viewership_Destination, T.partition_col_viewership)
        
    else:
        pass
