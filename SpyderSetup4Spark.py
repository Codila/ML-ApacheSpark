import os
import sys
os.chdir("D:/spark/Python_Scripts")
os.curdir
#Configure the environment.Set this up to the directory where spark is installed
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME']=   "D:/spark/spark-2.4.0-bin-hadoop2.7"
if 'JAVA_HOME' not in os.environ:
    os.environ['JAVA_HOME']= "C:\Program Files\Java\jre1.8.0_191"    
    
#Create a variable for our root path 
SPARK_HOME= os.environ['SPARK_HOME']


#Add the following path to the system path.Please check your installaation
#to make sure that these zip files actually exist. The namnes might change as version 
#changes 

sys.path.insert(0,os.path.join(SPARK_HOME,"python"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib","pyspark.zip"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","py4j-0.10.7-src.zip"))

#initiate spark context . once this is done all other appplication can run

from pyspark import SparkContext
from pyspark import SparkConf

#optionally configure SparkSettings
conf= SparkConf()
conf.set("spark.executor.memory","1g")
conf.set("spark.cores.max","2")

conf.setAppName("Spark-1st-Attempt")

#initialize spark context run only once otherwise you get  context error
sc= SparkContext('local',conf=conf)


#Test to make sure everything works

lines= sc.read.csv("creditcard.csv")
lines.first()

from pyspark.sql import SparkSession
spark = SparkSession.builder \
        .master('local') \
        .appName('Spark-1st-Attempt') \
        .getOrCreate()
df = spark.read.format('csv').option('header','true').option('mode','DROPMALFORMED').load('creditcard.csv')


