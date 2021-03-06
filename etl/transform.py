from pyspark.sql import SparkSession, SQLContext, GroupedData
from pyspark.sql.functions import *
from pyspark.sql.functions import date_add as d_add
from pyspark.sql.types import DoubleType
from etl.data_cleaning import valid_codes_dict

@udf()
def get_i94port(city):
    '''
    Input: City name
    Output: Corresponding i94port
    '''
    
    for key in valid_codes_dict:
        if city.lower() in valid_codes_dict[key][0].lower():
            return key

class Transformer:
    @staticmethod
    def demographics(demographics):
        """
        Function to transform demographics dataset grouping by state an calculate 
        all the totals and ratios for every race in every state
        Input: demographics dataset
        Output: demographics dataset transformed
        """
        demo = demographics \
            .groupBy(col("State Code").alias("State_code"), col("State")).agg(
            sum("Total Population").alias("Total_Population")\
            , sum("Male Population").alias("Male_Population"), sum("Female Population")
            .alias("Female_Population")\
            , sum("American Indian and Alaska Native").alias("American_Indian_and_Alaska_Native")\
            , sum("Asian").alias("Asian"), sum("Black or African-American").alias("Black_or_African-American")\
            , sum("Hispanic or Latino").alias("Hispanic_or_Latino")\
            , sum("White").alias("White")) \
            .withColumn("Male_Population_Ratio", round(col("Male_Population") / col("Total_Population"), 2))\
            .withColumn("Female_Population_Ratio", round(col("Female_Population") /
                                                         col("Total_Population"), 2))\
            .withColumn("American_Indian_and_Alaska_Native_Ratio",
                        round(col("American_Indian_and_Alaska_Native") / col("Total_Population"), 2))\
            .withColumn("Asian_Ratio", round(col("Asian") / col("Total_Population"), 2))\
            .withColumn("Black_or_African-American_Ratio",
                        round(col("Black_or_African-American") / col("Total_Population"), 2))\
            .withColumn("Hispanic_or_Latino_Ratio", round(col("Hispanic_or_Latino") / 
                                                          col("Total_Population"), 2))\
            .withColumn("White_Ratio", round(col("White") / col("Total_Population"), 2))

        return demo

    @staticmethod
    def immigrants(immigrants):
        """
        Function to transform inmigration dataset on order to get arrival date 
        in different columns (year, month, day) for the dataset
        Input: immigration dataset
        Output: immigration dataset transformed
        """
        immigrants = immigrants \
            .withColumn("arrival_date-split", split(col("arrival_date"), "-")) \
            .withColumn("arrival_year", col("arrival_date-split")[0]) \
            .withColumn("arrival_month", col("arrival_date-split")[1]) \
            .withColumn("arrival_day", col("arrival_date-split")[2]) \
            .drop("arrival_date-split")

        return immigrants
    
    @staticmethod
    def temperatue(city_temperature):
        """
        Function to add I94port in city temperature data with the mapping of valid codes.
        """
        city_temperature = city_temperature.withColumn("cod_port", get_i94port(city_temperature.City))
        city_temperature = city_temperature.filter(city_temperature.cod_port != 'null')
        
        return city_temperature