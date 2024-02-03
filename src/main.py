from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window, SparkSession
import json
import os
from analysis import Case_Study



if __name__ == "__main__":
    try:
        #Creating Spark Sessions
        spark = SparkSession.builder.appName("BCGX_CAR_CRASH_ANALYSIS").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        # os.environ["SPARK_HOME"] = "C:\Users\Pradeep\spark\spark-3.5.0-bin-hadoop3"
        os.environ["HADOOP_HOME"] = "C:/Users/Pradeep/anaconda3/pkgs/pyspark-3.4.1-py312haa95532_0/Lib/site-packages/pyspark/hadoop/bin/winutils.exe"
        
        with open("./config.json", "r") as json_file:
            config = json.load(json_file)

        cs = Case_Study(spark, config)
        cs.Analysis_1()
        
        
    except Exception as e:
        raise Exception(e)
        
############################################################## END OF FILE ##################################################################
    
    
