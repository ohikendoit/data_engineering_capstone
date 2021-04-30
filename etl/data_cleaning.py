from pyspark.sql import SparkSession, SQLContext, GroupedData
from pyspark.sql.functions import *
from pyspark.sql.functions import date_add as d_add
from pyspark.sql.types import DoubleType
import re

# Data cleaning to create a dictionary of valid i94port codes
rgex = re.compile(r'\'(.*)\'.*\'(.*)\'')

valid_codes_dict = {}
with open('reference.txt') as f:
    for line in f:
        match = rgex.search(line)
        valid_codes_dict[match[1]]=[match[2]]

class Cleaner:
    @staticmethod
    def get_cities_demographics(demographics):
        """
        Function to process null values and grouping by city and state and pivot
        Input: demographics dataset
        Output: demographics dataset cleaned
        """
        pivot = demographics.groupBy(col("City"), col("State"), col("Median Age"), col("Male Population"),
                                     col("Female Population") \
                                     , col("Total Population"), col("Number of Veterans"), col("Foreign-born"),
                                     col("Average Household Size") \
                                     , col("State Code")).pivot("Race").agg(sum("count").cast("integer")) \
            .fillna({"American Indian and Alaska Native": 0,
                     "Asian": 0,
                     "Black or African-American": 0,
                     "Hispanic or Latino": 0,
                     "White": 0})

        return pivot

    @staticmethod
    def get_airports(airports):
        """
        Function to filter only the US located  airports
        Input: airports dataframe
        Output: airports dataframe cleaned
        """
        airports = airports \
            .where(
            (col("iso_country") == "US") & (col("type").isin("large_airport", "medium_airport", "small_airport"))) \
            .withColumn("iso_region", substring(col("iso_region"), 4, 2)) \
            .withColumn("elevation_ft", col("elevation_ft").cast("float"))

        return airports

    @staticmethod
    def get_immigration(immigration):
        """
        Function to rename the columns with understandable names
        Input: inmigrantion dataset
        Output: inmigrantion dataset cleaned
        """
        immigration = immigration.filter(immigration.i94port.isin(list(valid_codes_dict.keys())))
        immigration = immigration \
            .withColumn("cic_id", col("cicid").cast("integer")) \
            .drop("cicid") \
            .withColumnRenamed("i94addr", "cod_state") \
            .withColumnRenamed("i94port", "cod_port") \
            .withColumn("cod_visa", col("i94visa").cast("integer")) \
            .drop("i94visa") \
            .withColumn("cod_mode", col("i94mode").cast("integer")) \
            .drop("i94mode") \
            .withColumn("cod_country_origin", col("i94res").cast("integer")) \
            .drop("i94res") \
            .withColumn("cod_country_cit", col("i94cit").cast("integer")) \
            .drop("i94cit") \
            .withColumn("year", col("i94yr").cast("integer")) \
            .drop("i94yr") \
            .withColumn("month", col("i94mon").cast("integer")) \
            .drop("i94mon") \
            .withColumn("bird_year", col("biryear").cast("integer")) \
            .drop("biryear") \
            .withColumn("age", col("i94bir").cast("integer")) \
            .drop("i94bir") \
            .withColumn("counter", col("count").cast("integer")) \
            .drop("count") \
            .withColumn("data_base_sas", to_date(lit("01/01/1960"), "MM/dd/yyyy")) \
            .withColumn("arrival_date", expr("date_add(data_base_sas, arrdate)")) \
            .withColumn("departure_date", expr("date_add(data_base_sas, depdate)")) \
            .drop("data_base_sas", "arrdate", "depdate")

        return immigration.select(col("cic_id"), col("cod_port"), col("cod_state"),
                                  col("visapost"),col("matflag"), col("dtaddto") \
                                  , col("gender"), col("airline"), col("admnum"), col("fltno"),
                                  col("visatype"), col("cod_visa"), col("cod_mode") \
                                  , col("cod_country_origin"), col("cod_country_cit"), col("year"),
                                  col("month"), col("bird_year") \
                                  , col("age"), col("counter"), col("arrival_date"), col("departure_date"))

    @staticmethod
    def get_temperature(temperature_df):
        """
        Function to filter out 'NaN' values, drop the duplicated values by city and country.
        """
        # filter NaN values
        temperature_df = temperature_df.filter(temperature_df.AverageTemperature != 'NaN')
        
        # select United States data for I94 immigration data.
        temperature_df = temperature_df.filter(temperature_df.Country == 'United States')

        # removed duplicated locations.
        temperature_df = temperature_df.dropDuplicates(['City', 'Country'])
        return temperature_df