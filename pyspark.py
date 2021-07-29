from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import to_timestamp
import urllib.request
import os

nyc_path = "../data"
yellowTaxiDataPath = "../data/yellow"
greenTaxiDataPath = "../data/green"

# make dir if it does not exist
if not os.path.isdir(nyc_path):
    os.mkdir(nyc_path)
if not os.path.isdir(yellowTaxiDataPath):
    os.mkdir(yellowTaxiDataPath)
if not os.path.isdir(greenTaxiDataPath):
    os.mkdir(greenTaxiDataPath)

# TODO
# Create a scrapper using beautiful soap to scrap directly from NYC TLC website instead of s3 bucket
# s3 bucket where data is stored
baseUrl = "https://s3.amazonaws.com/nyc-tlc/trip+data/"

nyc_schema = StructType([
    StructField('Vendor', StringType(), True),
    StructField('Pickup_DateTime', TimestampType(), True),
    StructField('Dropoff_DateTime', TimestampType(), True),
    StructField('Passenger_Count', IntegerType(), True),
    StructField('Trip_Distance', DoubleType(), True),
    StructField('Pickup_Longitude', DoubleType(), True),
    StructField('Pickup_Latitude', DoubleType(), True),
    StructField('Rate_Code', StringType(), True),
    StructField('Store_And_Forward', StringType(), True),
    StructField('Dropoff_Longitude', DoubleType(), True),
    StructField('Dropoff_Latitude', DoubleType(), True),
    StructField('Payment_Type', StringType(), True),
    StructField('Fare_Amount', DoubleType(), True),
    StructField('Surcharge', DoubleType(), True),
])

# yellow taxi
yellowTaxiPrefix = "yellow_tripdata_"
yellowTaxiDict = {}
yellowTaxiUrls = []
yellowTaxiFilenames = []

# green taxi
greenTaxiPrefix = "green_tripdata_"
greenTaxiDict = {}
greenTaxiUrls = []
greenTaxiFilenames = []

# Availability of data set by month & year


def defineRange(startyear, endyear):
    years = range(startyear, endyear+1)
    months = range(1, 13)
    for year in years:
        yellowTaxiDict[year] = months
        greenTaxiDict[year] = months


defineRange(2018, 2020)


def generateUrlsandFileNames():
    for year, monthList in yellowTaxiDict.items():
        yearStr = str(year)
        for month in monthList:
            monthStr = str(month)
            if len(monthStr) == 1:
                monthStr = "0"+monthStr
            yellowTaxiUrl = baseUrl+yellowTaxiPrefix+yearStr+'-'+monthStr+".csv"
            yellowTaxiUrls.append(yellowTaxiUrl)
            yellowTaxiFilenames.append(
                yellowTaxiPrefix+yearStr+'-'+monthStr+".csv")
            greenTaxiUrl = baseUrl+greenTaxiPrefix+yearStr+'-'+monthStr+".csv"
            greenTaxiUrls.append(greenTaxiUrl)
            greenTaxiFilenames.append(
                greenTaxiPrefix+yearStr+'-'+monthStr+".csv")


def save(url, filename):
    urllib.request.urlretrieve(url, filename)


def downloadDataFromS3Bucket():
    for url, filename in zip(yellowTaxiUrls, yellowTaxiFilenames):
        print(url, filename)
        save(url, nyc_path + '/yellow/' + filename)

    for url, filename in zip(greenTaxiUrls, greenTaxiFilenames):
        print(url, filename)
        save(url, nyc_path + '/green/' + filename)


def createSparkSession():
    spark = SparkSession.builder \
        .master("local") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark


def changeColumntoDateTime(df, column, new_column):
    df.select(to_timestamp(
        column, 'yyyy-MM-dd HH:mm:ss').alias(new_column)).collect()
    return df


def csvToParquet(df, path):
    df.write.parquet(path)


def csvToAvro(df, path):
    df.write.format("avro").save(path)


generateUrlsandFileNames()
downloadDataFromS3Bucket()
spark = createSparkSession()

# TODO
# define own scheme when reading
# example using a header file

dfGreenTaxi = spark.read.option("header", "true").csv(nyc_path+"/green/*.csv")
dfYellowTaxi = spark.read.option(
    "header", "true").csv(nyc_path+"/yellow/*.csv")

dfGreenTaxi.show()
dfYellowTaxi.show()

dfGreenTaxiWithDateTimePickUp = changeColumntoDateTime(
    dfGreenTaxi, dfGreenTaxi.lpep_pickup_datetime, 'pickup_datetime')
dfGreenTaxiWithDateTimeDropOff = changeColumntoDateTime(
    dfGreenTaxiWithDateTimePickUp, dfGreenTaxiWithDateTimePickUp.lpep_dropoff_datetime, 'dropoff_datetime')

dfYellowTaxiWithDateTimePickUp = changeColumntoDateTime(
    dfYellowTaxi, dfYellowTaxi.tpep_pickup_datetime, 'pickup_datetime')
dfYellowTaxiWithDateTimeDropOff = changeColumntoDateTime(
    dfYellowTaxiWithDateTimePickUp, dfYellowTaxiWithDateTimePickUp.tpep_dropoff_datetime, 'dropoff_datetime')


csvToParquet(dfGreenTaxiWithDateTimeDropOff, nyc_path+"\greenTaxi.parquet")
csvToParquet(dfYellowTaxiWithDateTimeDropOff, nyc_path+"\yellowTaxi.parquet")

csvToAvro(dfGreenTaxiWithDateTimeDropOff, nyc_path+"\green.avro")
csvToAvro(dfYellowTaxiWithDateTimeDropOff, nyc_path+"\yellow.avro")

# Queries

# The average distance driven by yellow and green taxis per hour
query1 = "SELECT 'yellow_taxi' as yellow_taxi,AVG(trip_distance) AS avg_distance_miles, \
            HOUR(pickup_datetime) AS hour, \
            FROM dfYellowTaxiWithDateTimeDropOff \
            WHERE  trip_distance > 0 \
            AND dropoff_datetime > pickup_datetime \
            GROUP BY hour \
            ORDER BY hour \
            UNION \
            SELECT 'Green_Taxi' as green_taxi, AVG(trip_distance) AS avg_distance_miles, \
            HOUR(pickup_datetime) AS hour, \
            FROM dfGreenTaxiWithDateTimeDropOff \
            WHERE  trip_distance > 0 \
            AND dropoff_datetime > pickup_datetime \
            GROUP BY hour \
            ORDER BY hour"

# Day of the week in 2019 and 2020 which has the lowest number of single rider trips
query2 = "SELECT taxi_color, Week, day_of_week, min(lowest_num_of_trip) \
            FROM ( \
                SELECT 'yellow taxi' as taxi_color, WEEK(_pickup_datetime) as week, \
                DAYOFWEEK(pickup_datetime) as dayOfWeek, \
                count(*) as LowestNumOfTrips \
                FROM dfYellowTaxiWithDateTimeDropOff \
                WHERE passenger_count=1 \
                AND year(pickup_datetime) in ('2019','2020')  \
                GROUP BY  'yellow taxi', \
                WEEK(pickup_datetime), \
                DAYOFWEEK(pickup_datetime) \
            UNION \
                SELECT 'green taxi' as taxi_color, WEEK(pickup_datetime) as week, \
                DAYOFWEEK(pickup_datetime) as day_of_week, \
                count(*) as LowestNumOfTrips \
                FROM dfGreenTaxiWithDateTimeDropOff \
                WHERE passenger_count=1 \
                AND year(pickup_datetime) in ('2019','2020')  \
                GROUP BY  'green taxi', \
                WEEK(pickup_datetime), \
                DAYOFWEEK(pickup_datetime) \
                ) \
            GROUP BY taxi_color, week"

# The top 3 of the busiest hours
query3 = "SELECT taxi, color, busiest_hour, no_of rides \
            FROM ( \
                SELECT 'yellow_taxi' as taxi_color, HOUR(pickup_datetime) as busiest_hour, count(*) as no_of_rides \
                FROM dfYellowTaxiWithDateTimeDropOff \
                GROUP BY HOUR(pickup_datetime) \
            UNION \
                SELECT 'green_taxi' as taxi_color, HOUR(pickup_datetime) as busiest_hour, count(*) as no_of_rides \
                FROM dfGreenTaxiWithDateTimeDropOff \
                GROUP BY HOUR(pickup_datetime) \
                ) \
            ORDER by count(*) desc \
            LIMIT 3"
