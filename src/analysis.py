"""
    Title        : CAR_CRASH_ANALYSIS_JOB
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
        try:
            print("\n ANALYSIS BEGINS HERE \n")

            print("\n###############################################  ANALYSIS 1 ###################################################")
            print("\nANALYSIS OBJECTIVE: Find the number of crashes (accidents) in which number of males killed are greater than 2")
            a1_01 = self.primary_person_df.where(trim(col("PRSN_INJRY_SEV_ID")) == "KILLED").groupBy("CRASH_ID", "PRSN_GNDR_ID").agg(count("*").alias("CRASH_GENDERWISE_FATALITY_COUNT"))
            a1_02 = a1_01.where((col("CRASH_GENDERWISE_FATALITY_COUNT") > 2) & (trim(col("PRSN_GNDR_ID")) == "MALE"))            
            print("\nANSWER:", a1_02.count())
            print("\n############################################# END OF ANALYSIS 1 ###############################################")
            # a1_02.coalesce(1).write.format("csv").mode("overwrite").save("./data/processed_data/analysis_1/")


            print("\n\n###############################################  ANALYSIS 2 ###################################################")
            print("\nANALYSIS OBJECTIVE: How many two wheelers are booked for crashes?")
            two_wheelers = ["MOTORCYCLE", "POLICE MOTORCYCLE"]
            a2_01 = self.units_df.where(trim(col("VEH_BODY_STYL_ID")).isin(two_wheelers))
            a2_02 = a2_01.select("CRASH_ID", "UNIT_NBR").distinct()
            print("\nANSWER:", a2_02.count())
            print("\n############################################# END OF ANALYSIS 2 ###############################################")


            print("\n\n###############################################  ANALYSIS 3 ###################################################")
            print("\nANALYSIS OBJECTIVE: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy")
            # get crash-specific info
            a3_01 = self.primary_person_df.where((trim(col("PRSN_TYPE_ID")) == "DRIVER") & (trim(col("PRSN_INJRY_SEV_ID")) == "KILLED") & (trim(col("PRSN_AIRBAG_ID")) == "NOT DEPLOYED"))
            a3_02 = a3_01.select("CRASH_ID", "UNIT_NBR").distinct()

            # get vehicle-specific info
            a3_03 = self.units_df.select("CRASH_ID", "UNIT_NBR", "VEH_MAKE_ID").distinct()

            # bring the two together
            a3_04 = a3_02.join(a3_03, on = ["CRASH_ID", "UNIT_NBR"], how = "left")

            a3_05 = a3_04.groupBy("VEH_MAKE_ID").agg(count('*').alias("COUNT_REQ_CRASHES_PER_MAKE")).orderBy(col("COUNT_REQ_CRASHES_PER_MAKE").desc())
            a3_06 = a3_05.select("VEH_MAKE_ID", "COUNT_REQ_CRASHES_PER_MAKE").limit(5)
            print("\nANSWER:\n")
            a3_06.show()
            print("\n############################################# END OF ANALYSIS 3 ###############################################")
        

            print("\n\n###############################################  ANALYSIS 4 ###################################################")
            print("\nANALYSIS OBJECTIVE: Determine number of vehicles with the driver having valid licences involved in a hit and run")
            # required license types
            valid_license_types = ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]

            # get legit licensed driver crash records
            a4_01 = self.primary_person_df.where(trim(col("DRVR_LIC_TYPE_ID")).isin(valid_license_types)).select("CRASH_ID", "UNIT_NBR").distinct()
            # get hit and run crash records
            a4_02 = self.units_df.where(trim(col("VEH_HNR_FL")) == "Y").select("CRASH_ID", "UNIT_NBR").distinct()

            # bring the two together
            a4_03 = a4_01.join(a4_02, on = ["CRASH_ID", "UNIT_NBR"], how = "inner")
            print("\nANSWER:", a4_03.count())
            print("\n############################################# END OF ANALYSIS 4 ###############################################")


            print("\n\n###############################################  ANALYSIS 5 ###################################################")
            print("\nANALYSIS OBJECTIVE: Which state has the highest number of accidents in which females are not involved?")
            # let's first seggregate the crashes doesn't involve women
            a5_01 = self.primary_person_df.groupBy("CRASH_ID").agg(collect_set("PRSN_GNDR_ID").alias("CRASH_GENDER_POOL"))
            a5_02 = a5_01.where((size(col("CRASH_GENDER_POOL")) == 1) & (trim(col("CRASH_GENDER_POOL")[0]) == "MALE")).select("CRASH_ID")

            # Let's now get the corresponding states for those crashes
            a5_03 = self.units_df.select("CRASH_ID", "VEH_LIC_STATE_ID").distinct()

            a5_04 = a5_02.join(a5_03, on = ["CRASH_ID"], how = "left").select("VEH_LIC_STATE_ID")
            a5_05 = a5_04.groupBy("VEH_LIC_STATE_ID").agg(count("*").alias("COUNT")).orderBy(col("COUNT").desc())
            a5_06 = a5_05.limit(1).select("VEH_LIC_STATE_ID","COUNT")
            print("\nANSWER:")
            a5_06.show()
            print("\n############################################# END OF ANALYSIS 5 ###############################################")


            print("\n\n###############################################  ANALYSIS 6 ###################################################")
            print("\nANALYSIS OBJECTIVE: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death?")
            # records corresponding to all possible injuries
            req_injury_sev = ["NOT INJURED", "NA", "UNKNOWN"]
            a6_01 = self.primary_person_df.where(~trim(col("PRSN_INJRY_SEV_ID")).isin(req_injury_sev)).select("CRASH_ID", "UNIT_NBR").distinct()

            # get the make_id of vehicles involved in all crashes
            a6_02 = self.units_df.select("CRASH_ID", "UNIT_NBR", "VEH_MAKE_ID").distinct()

            #bring the two together
            a6_03 = a6_01.join(a6_02, on = ["CRASH_ID", "UNIT_NBR"], how = "left")
            a6_04 = a6_03.groupBy("VEH_MAKE_ID").agg(count("*").alias("COUNT_BY_MAKE_ID"))

            # Get those between Rank 3 & 5
            w2 = Window.orderBy(col("COUNT_BY_MAKE_ID").desc())
            a6_05 = a6_04.withColumn("RANK_BY_CRASH_COUNT", rank().over(w2))
            a6_06 = a6_05.select("VEH_MAKE_ID","RANK_BY_CRASH_COUNT").where((col("RANK_BY_CRASH_COUNT") > 2))
            a6_07 = a6_06.where((col("RANK_BY_CRASH_COUNT") < 6))
            print("\nANSWER:")
            a6_07.show()
            print("\n############################################# END OF ANALYSIS 6 ###############################################")


            print("\n\n###############################################  ANALYSIS 7 ###################################################")
            print("\nANALYSIS OBJECTIVE: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style")
            # get body-style info for each crash unit
            a7_01 = self.units_df.select("CRASH_ID", "UNIT_NBR", "VEH_BODY_STYL_ID").distinct()

            # get ethnicity of person involved in crash
            a7_02 = self.primary_person_df.select("CRASH_ID", "UNIT_NBR", "PRSN_ETHNICITY_ID").distinct()

            # bring them together
            a7_03 = a7_01.join(a7_02, on = ["CRASH_ID", "UNIT_NBR"], how = "inner").drop("CRASH_ID", "UNIT_NBR")

            a7_04 = a7_03.groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").agg(count("*").alias("count_ethnic_bdystyl_group"))#.show()

            w3 = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count_ethnic_bdystyl_group").desc())
            a7_05 = a7_04.withColumn("rank_ethnic_bdystyl_group", rank().over(w3))
            a7_06 = a7_05.where(col("rank_ethnic_bdystyl_group") == 1).select("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
            a7_07 = a7_06.withColumnRenamed("PRSN_ETHNICITY_ID", "TOP_ETHNIC_USER")
            print("\nANSWER:")
            a7_07.show(truncate = False)
            print("\n############################################# END OF ANALYSIS 7 ###############################################")


            print("\n\n###############################################  ANALYSIS 8 ###################################################")
            print("\nANALYSIS OBJECTIVE: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)?")
            alcohol_contrib_factors = ["UNDER INFLUENCE - ALCOHOL", "HAD BEEN DRINKING"]

            # get crash-victims with alcohol as the contributing factor
            a8_01 = self.units_df.where(trim(col("CONTRIB_FACTR_1_ID")).isin(alcohol_contrib_factors)).select("CRASH_ID", "UNIT_NBR").distinct()
            # get zip info of crash-victims
            a8_02 = self.primary_person_df.select("CRASH_ID", "UNIT_NBR", "DRVR_ZIP").distinct()

            # bring both together
            a8_03 = a8_01.join(a8_02, on = ["CRASH_ID", "UNIT_NBR"], how = "left")

            # get the count of alcohol induced crashes by driver zip
            a8_04 = a8_03.groupBy("DRVR_ZIP").agg(count('*').alias("DND_CRASH_COUNT_ZIP")).orderBy(col("DND_CRASH_COUNT_ZIP").desc()).limit(6)
            a8_05 = a8_04.where(col("DRVR_ZIP").isNotNull()).distinct()
            print("\nANSWER:")
            a8_05.show(truncate = False)
            print("\n############################################# END OF ANALYSIS 8 ###############################################")

            print("\n\n###############################################  ANALYSIS 9 ###################################################")
            print("\nANALYSIS OBJECTIVE: Count of Distinct Crash IDs where no damaged property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails insurance")
            req_damage_levels = ["DAMAGED 5","DAMAGED 6","DAMAGED 7 HIGHEST"]
            # get crashes with req damage levels and valid insurance 
            a9_01 = self.units_df.where((trim(col('VEH_DMAG_SCL_1_ID')).isin(req_damage_levels) | trim(col('VEH_DMAG_SCL_2_ID')).isin(req_damage_levels)))
            a9_02 = a9_01.where(trim(col('FIN_RESP_TYPE_ID')).contains("INSURANCE")).select("CRASH_ID").distinct()

            # get no damage crashes from above with an anti-left join with damages_df
            a9_03 = self.damages_df.select("CRASH_ID").distinct()

            a9_04 = a9_02.join(a9_03, on = ["CRASH_ID"], how = 'leftanti')
            print("\n ANSWER:", a9_04.count())
            print("\n############################################# END OF ANALYSIS 9 ###############################################")

            print("\n\n###############################################  ANALYSIS 10 #################################################")
            print("\nANALYSIS OBJECTIVE: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)")
            # get account of drivers charged with speeding related offense
            a10_01 = self.charges_df.filter(col('CHARGE').contains('SPEED')).select('CRASH_ID','UNIT_NBR','CHARGE')

            # get top 10 used vehicle colors
            a10_02 = self.units_df.filter("VEH_COLOR_ID != 'NA'").select('VEH_COLOR_ID').groupBy('VEH_COLOR_ID').agg(count('*').alias("COUNT_BY_COLORS"))
            a10_03 = a10_02.withColumn('RANK_TOP_COLORS',rank().over(Window.orderBy(col('COUNT_BY_COLORS').desc()))).filter(col("RANK_TOP_COLORS") <= 10).select('VEH_COLOR_ID')

            # get top 25 states
            a10_04 = self.units_df.filter(~col('VEH_LIC_STATE_ID').isin(['NA','Unknown','Other'])).select('VEH_LIC_STATE_ID').groupBy('VEH_LIC_STATE_ID').agg(count('*').alias("COUNT_BY_STATE"))
            a10_05 = a10_04.withColumn('RANK_TOP_STATES',rank().over(Window.orderBy(col('COUNT_BY_STATE').desc()))).filter(col("RANK_TOP_STATES") <= 25).select('VEH_LIC_STATE_ID')

            # get crashes corresponding to top 10 colors
            a10_06 = self.units_df.join(a10_03,on=['VEH_COLOR_ID'],how='inner').select('CRASH_ID','UNIT_NBR','VEH_MAKE_ID','VEH_LIC_STATE_ID')

            # further, get crashes corresponding to speeding related offense
            a10_07 = a10_06.join(a10_01,on=['CRASH_ID','UNIT_NBR'],how='inner').select('CRASH_ID','UNIT_NBR','VEH_MAKE_ID','VEH_LIC_STATE_ID')

            #furthur, get the crashes that occured only in the top 25 states
            a10_08 = a10_07.join(a10_05, on=['VEH_LIC_STATE_ID'],how='inner').select('CRASH_ID','UNIT_NBR','VEH_MAKE_ID')

            #crashes corresponding to licensed drivers
            a10_09 = self.primary_person_df.filter(~trim(col("DRVR_LIC_CLS_ID")).isin(['UNLICENSED', "NA", "UNKNOWN"])).select('CRASH_ID','UNIT_NBR')

            #bring them together
            a10_10 = a10_09.join(a10_08,on=['CRASH_ID','UNIT_NBR'],how='inner').select('VEH_MAKE_ID').groupBy('VEH_MAKE_ID').agg(count('*').alias("MAKEWISE_COUNT")) 
            a10_11 = a10_10.orderBy(col("MAKEWISE_COUNT").desc()).limit(5)

            print("\nANSWER:")
            a10_11.show()
            print("\n############################################# END OF ANALYSIS 10 ##############################################")

            print("\nANALYSIS ENDS HERE!")
        except Exception as e:
            print('Exception: ',e)
        
        
            