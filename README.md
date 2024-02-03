                                                        BCGX - CASE STUDY - CAR CRASH ANALYSIS
                                                                         BY
                                                                   PRADEEP GANESAN

**SUMMARY**
The project consists of a case study on car crashes in the US. The case study is done as part of the recruitment process at BCGX. The entire project is developed with pyspark and python. The main.py is the file that can be executed from command line or via spark submit to initiate the analysis job. The output of the 10 core analytic propositions can be viewed directly at the terminal by simply just running the main.py function from the terminal.

**HOW TO RUN**

**ANALYTICS REQUIREMENTS**
1.	Find the number of crashes (accidents) in which number of males killed are greater than 2?
2.	How many two wheelers are booked for crashes? 
3.	Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
4.	Determine number of Vehicles with driver having valid licences involved in hit and run? 
5.	Which state has highest number of accidents in which females are not involved? 
6.	Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
7.	For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
8.	Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
9.	Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
10.	Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

**EXPECTED OUTPUT**
1.	Develop an application which is modular & follows software engineering best practices (e.g. Classes, docstrings, functions, config driven, command line executable through spark-submit)
2.	Code should be properly organized in folders as a project.
3.	Input data sources and output should be config driven
4.	Code should be strictly developed using Data Frame APIs (Do not use Spark SQL)
5.	Share the entire project as zip or link to project in GitHub repo.


**PROJECT STRUCTURE:**

    CASE_STUDY_BCGX:.
    │   BCGX_CAR_CRASH_ANALYSIS.ipynb
    │   config.json
    │   README.md
    │   requirements.txt
    │
    ├───data
    │   ├───raw_data
    │   │       Charges_use.csv
    │   │       Damages_use.csv
    │   │       Endorse_use.csv
    │   │       Primary_Person_use.csv
    │   │       Restrict_use.csv
    │   │       Units_use.csv
    │   │
    │   └───support_files
    │           BCG_Case_Study_CarCrash_Updated_Questions.docx
    │           Data Dictionary.xlsx
    │
    ├───src
    │   │   analysis.py
    │   │   main.py
    │   │
    │   └───__pycache__
    │           analysis.cpython-312.pyc
    │
    └───temp
