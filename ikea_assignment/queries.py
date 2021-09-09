from pyspark.sql import SparkSession

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType, ArrayType, MapType

from pyspark.sql.window import Window

spark = SparkSession.builder \
    .enableHiveSupport() \
    .appName("SimpleApp") \
    .getOrCreate()


class Queries:
    """
    Class demonstrates some basic queries from the assignment.
    These are presented in two forms, using PySpark and SQL syntax.
    """

    def __init__(self, df):
        """
        creates an object for the given DF
        :param df: Pyspark DF
        """
        self.df = df

    def most_sold_products_pyspark(self):
        """
        returns the most sold product in the DF
        :return:
        """

        self.df\
            .select(['ID', 'quantity']).groupBy('ID') \
            .agg(F.sum(F.col('quantity')).alias("total_amount")) \
            .orderBy('total_amount', ascending=False) \
            .show()

        return


    def most_sold_products_sql(self):
        """
        returns the most sold products  using SQL query
        :return:
        """

        if 'example' in [table.name for table in spark.catalog.listTables()]:
            print('deleting')
            spark.catalog.dropTempView("example")

        self.df.createTempView("example")


        query = ("""
        SELECT ID quantity,
        SUM (quantity)
        FROM example
        GROUP BY ID
        ORDER BY sum(quantity) DESC
        """)

        spark.sql(query).show()

        return


    def highest_revenue_date_pyspark(self):
        """
        returns the date for the highest revenue
        :return:
        """

        daily_revenue = self.df \
            .select(['timestamp', 'quantity', 'price']) \
            .groupBy(F.to_date(F.col('timestamp')).alias('date')) \
            .agg(F.sum(F.col('price') * F.col('quantity')).alias('daily_revenue')) \
            .orderBy('daily_revenue', ascending=False) \
            .withColumn("daily_revenue", F.round(F.col("daily_revenue"), 2))

        return daily_revenue


    def highest_revenue_date_sql(self):
        """
        returns the date for the highest revenue using SQL syntax
        :return:
        """
        if 'example' in [table.name  for  table in  spark.catalog.listTables()]:
            print('deleting/recreating view')
            spark.catalog.dropTempView("example")

        self.df.createTempView("example")


        query = ("""
        SELECT   CAST(timestamp AS DATE) as date, SUM(price*quantity) as pq
        FROM example
        GROUP BY date
        ORDER BY pq  DESC
        """)

        return spark.sql(query).show(5)


    def top_5_selling_each_day_pyspark(self):
        """
        returns top 5  selling products for each day using pyspark syntax

        :return:
        """

        window = Window.partitionBy('date').orderBy(F.col('total_amount').desc())


        tmp = \
            self.df\
                .groupBy([F.to_date(F.col('timestamp')).alias('date'), 'ID']) \
                .agg(F.sum('quantity').alias('total_amount')) \
                .orderBy('date', accending=False) \
                .withColumn('row_number', F.row_number().over(window)) \
                .filter(F.col('row_number') <= 5) \
                .withColumn('zipped', F.concat("ID", F.lit(':'), "total_amount")) \
                .groupBy('date').agg(F.collect_list('zipped').alias("product_id:number_sold")) \
                .orderBy('date') \
                .show(50, truncate=False)

        return tmp


    def top_5_selling_each_day_sql(self):
        """
        returns top 5  selling products for each day using pyspark syntax

        :return:
        """
        if 'example' in [table.name for table in spark.catalog.listTables()]:
            print('deleting')
            spark.catalog.dropTempView("example")

        self.df.createTempView("example")

        query = ("""

        SELECT date, ID, total_amount, rn
        FROM
            (SELECT  date, ID, total_amount,  ROW_NUMBER() OVER (PARTITION BY date
                                          ORDER BY total_amount DESC
                                         ) as rn
            FROM
                (SELECT CAST(timestamp AS DATE) as date, ID, sum(quantity) as total_amount
                FROM example
                GROUP  BY date, ID
                ) tmp


            ORDER BY date,rn
            )

        WHERE rn <= 5


        """)

        return spark.sql(query).show(20)