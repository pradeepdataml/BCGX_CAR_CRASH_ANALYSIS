from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window, SparkSession
import json
from analysis import Case_Study

"""
    Title        : MAIN.py
    Description  : A job-builder file that calls the analysis job when executed from terminal or via spark-submit.
    Created By   : Pradeep Ganesan
    Created Date : 02-Feb-2024
"""

if __name__ == "__main__":
    try:
        #Creating Spark Sessions
        spark = SparkSession.builder.appName("BCGX_CAR_CRASH_ANALYSIS").getOrCreate()
        spark.sparkContext.setLogLevel("FATAL")
    
        with open("./config.json", "r") as json_file:
            config = json.load(json_file)

        cs = Case_Study(spark, config)
        cs.start_analysis()

        spark.stop()
        
        
    except Exception as e:
        raise Exception(e)
        
############################################################## END OF FILE ##################################################################
    
    
