import os
import sys
os.chdir("D:/spark/Python_Scripts")
os.curdir
#Configure the environment.Set this up to the directory where spark is installed
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME']=   "D:/spark/spark-2.4.0-bin-hadoop2.7"
#if 'JAVA_HOME' not in os.environ:
#    os.environ['JAVA_HOME']= "C:\Program Files\Java\jre1.8.0_191"    
    
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
sc= SparkContext('local',conf=conf) # this is the instance of sparkContext that's how you 
#connect with Spark engine


#Test to make sure everything works

lines= sc.read.csv("creditcard.csv")
lines.first()

## Making a SQL query with Spark
from pyspark.sql import SparkSession
spark = SparkSession.builder \
        .master('local') \
        .appName('Spark-1st-Attempt') \
        .getOrCreate()
        
spark1 = SparkSession.builder.appName("Test").getOrCreate()    
df1 = spark1.read.format("com.databricks.spark.csv").option("header", "true").option("sep","\t")\
        .load("D:/Scala_eXCERSES/lastfm-dataset-1K/userid-timestamp-artid-artname-traid-traname.tsv")

df1.show(100)    

#In order to use SQL commands in SparkSQL we’ll need to register the data as a table

df1.registerTempTable("MusicPlay")
#Finally, we can use SQL commands by invoking the spark.sql function and our command in quotes as an argument:

result= spark1.sql("SELECT * FROM MusicPlay LIMIT 5").show()


#df creation
df = spark.read.format('csv').option('header','true').option('mode','DROPMALFORMED').load('creditcard.csv')
print(df.count())


# count words in pySpark
text_file= sc.textFile("C:/Users/Varun-PC/Desktop/Spark.txt")
counts= text_file.flatMap(lambda line: line.split(" ")) \
                .map(lambda word:(word,1)) \
                .reduceByKey(lambda a, b: a+b)
counts.saveAsTextFile("C:/Users/Varun-PC/Desktop/Sparkout.txt")


             