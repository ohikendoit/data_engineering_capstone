from pyspark.sql.functions import *


class QualityCheck:
    """
    Functions for quality checks on dimension and fact tables
    """

    def __init__(self, spark, paths):
        self.spark = spark
        self.paths = paths

    def demographics(self):
        """
        Function to get demographics dimension
        Output: demographics dimension
        """
        return self.spark.read.parquet(self.paths["demographics"])

    def airports(self):
        """
        Function to get airports dimension
        Output: airports dimension
        """
        return self.spark.read.parquet(self.paths["airports"])

    def temperature(self):
        """
        Function to get temperature dimension
        Output: temperature dimension
        """
        return self.spark.read.parquet(self.paths["temperature"])

    def get_facts(self):
        """
        Function to get facts table
        Output: facts table
        """
        return self.spark.read.parquet(self.paths["facts"])

    def get_dimensions(self):
        """
        Function to get all dimensions of the model
        Output: all dimensions
        """
        return self.demographics(), self.airports(), self.temperature()

    def row_count_check(self, dataframe):
        """
        Function to check empty dataframe
        Input: dataframe
        Output: true or false if the dataset has any row
        """
        return dataframe.count() > 0

    def integrity_checker(self, fact, dim_demographics, dim_airports, dim_temperature):
        """
        Function to check if all the facts columns joined with the dimensions has correct values 
        Input: fact table
        Input: demographics dimension
        Input: airports dimension
        Input: temperature dimension
        Output: true or false if integrity is correct.
        """
        integrity_demo = fact.select(col("cod_state")).distinct() \
                             .join(dim_demographics, fact["cod_state"] == 
                                   dim_demographics["State_Code"], "left_anti") \
                             .count() == 0

        integrity_airports = fact.select(col("cod_port")).distinct() \
                                 .join(dim_airports, fact["cod_port"] == 
                                       dim_airports["local_code"], "left_anti") \
                                 .count() == 0

        integrity_temperature = fact.select(col("cod_port")).distinct() \
                             .join(dim_temperature, fact["cod_port"] == 
                                   dim_temperature["cod_port"], "left_anti") \
                             .count() == 0

        return integrity_demo & integrity_airports & integrity_temperature