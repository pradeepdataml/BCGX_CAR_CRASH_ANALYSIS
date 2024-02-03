"""
    Title        : BCGX_CASE_STUDY
    Description  : A detailed analysis of car-crash related datasets native to the US.
    Created By   : Pradeep Ganesan
    Created Date : 02-Feb-2024
"""

from pyspark.sql import Window,SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class Case_Study():
    def __init__(self, spark, config):
        input_paths = config["input_data"]
        input_format = config["data_formats"]["input_format"]

        self.output_paths = config["output_data"]
        self.output_format = config["data_formats"]["input_format"]

        # reading raw data
        self.charges_df = spark.read.format(input_format).option("header", "true").option("inferSchema", "true").load(input_paths['Charges'])
        self.damages_df = spark.read.format(input_format).option("header", "true").option("inferSchema", "true").load(input_paths['Damages'])
        self.endorse_df = spark.read.format(input_format).option("header", "true").option("inferSchema", "true").load(input_paths['Endorse'])
        self.primary_person_df = spark.read.format(input_format).option("header", "true").option("inferSchema", "true").load(input_paths['Primary_Person'])
        self.units_df = spark.read.format(input_format).option("header", "true").option("inferSchema", "true").load(input_paths['Units'])
        self.restrict_df = spark.read.format(input_format).option("header", "true").option("inferSchema", "true").load(input_paths['Restrict'])
        

    def Analysis_1(self):
        """
        Question: Find the number of crashes (accidents) in which number of males killed are greater than 2?
        Input: DF
        output: DF
        """
        
        try:
            a1_01 = self.primary_person_df.where(trim(col("PRSN_INJRY_SEV_ID")) == "KILLED").groupBy("CRASH_ID", "PRSN_GNDR_ID").agg(count("*").alias("CRASH_GENDERWISE_FATALITY_COUNT"))
            a1_02 = a1_01.where((col("CRASH_GENDERWISE_FATALITY_COUNT") > 2) & (trim(col("PRSN_GNDR_ID")) == "MALE"))
            a1_02.show()
            print("abcd:", a1_02.count())
            a1_02.write.format("parquet").mode("write").save("data/processed_data/analysis_1/")

        except Exception as e:
            print('Exception: ',e)
        
        
            