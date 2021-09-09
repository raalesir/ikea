"""Top-level package for Ikea_assignment."""

__author__ = """Alexey Siretskiy"""
__email__ = 'alexey.siretskiy@gmail.com'
__version__ = '0.1.0'

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .enableHiveSupport() \
    .appName("SimpleApp") \
    .getOrCreate()



