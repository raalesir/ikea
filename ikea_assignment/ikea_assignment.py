"""`ikea_assignment` package."""

from pyspark.sql import SparkSession

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType, ArrayType, MapType



INPUT = "data/input/dataset.jsonl"
OUTPUT = "data/output/data.parquet"
STREAM_DATA = 'data/input_stream'
checkpointLocation = "file:///tmp/stream/checkpoint"

# from ikea_assignment import  spark

class  BasicPipeline:
    """
    Class responsible for the basic data pipeline  creation
    """

    def __init__(self, input_data,  spark_,  *args, **kwargs):
        """
        creates  data object

        :param input_data: path to the data
        :param spark_: SparkSession instance  (used for  testing)

        """
        self.data = input_data
        self.spark = spark_
        self.schema = self.get_schema()
        self.df = self.read_dataset()


    def get_schema(self):
        """
        returns the  data schema. In case the schema changes  in future, one can redefine the method

        :return: data schema
        """
        schema = StructType([
            StructField("user", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("items", ArrayType(
                        MapType(
                            StringType(),
                            MapType( StringType(), DoubleType() )
                        )
                    ), True),
        ])

        return schema


    def read_dataset(self):
        """
        returns the data as a PySpark  DF. It can be redefined if the data format changes. For example, if it shifts
        from `json` to something else.

        :return: PySpark DF
        """
        df_ = self.spark.read.json(self.data, schema=self.schema)

        return df_



    def transform(self):
        """
        transforms the data with the given `self.schema` to the format suitable for the SQL queries

        :return: Pyspark DF
        """
        self.df = self.df\
            .withColumn('items', F.explode('items')) \
            .withColumn('ID', F.map_keys("items")[0]) \
            .withColumn('qp', F.map_values('items')[0]) \
            .withColumn('quantity', F.col('qp').getItem('quantity').astype('int')) \
            .withColumn('price', F.col('qp').getItem('price')) \
            .select(['user', 'timestamp', 'ID', 'quantity', 'price'])


        return self.df


    def  save(self):
        """
        saves the transformed dataset.
        As an example, lets save it  as a Parquet. That columnar-based format is suited very good for  analycical
        queries. If other options preferred, this method could be overwritten in child classes.

        :return:

        """
        self.df.write.parquet(OUTPUT,  mode='overwrite')




class StreamPipeline(BasicPipeline):
    """
    streaming variant of the pipeline.
    It will monitor the STREAM_DATA directory, as soon as it discovers changes, like new data arrival, it will launch
    transformation and save the result in the OUTPUT.

    Before testing this make sure to issue to following:

    rm -rf /tmp/stream/checkpoint
    rm -rf data/output/data.parquet
    rm -rf data/input_stream/dataset.jsonl


    This resets the settings...
    """

    def __init__(self, input_data,  spark_, *args, **kwargs):
        super(StreamPipeline, self).__init__(input_data,  spark_, *args, **kwargs)

        self.schema = self.get_schema()
        self.df = self.read_dataset()


    def read_dataset(self):
        """
        Read the stream

        :return:
        """
        # df = self.spark.read.json(self.data, schema=self.schema)

        df_ = self.spark.readStream. \
            option("maxFileAge", 7776000000). \
            option("mode", "DROPMALFORMED"). \
            schema(schema=self.schema). \
            json(path=self.data)

        return df_


    def save(self):
        """
        Streaming version
        :return:
        """
        print("""
        saving the stream. It will wait 60 seconds and terminates...
        Put the  `dataset.jsonl` in the  `data_stream` directory.
        I.e. `cp %s %s`
        """%(INPUT, STREAM_DATA))

        query = self.df\
            .writeStream.format("parquet")\
                .option("checkpointLocation", checkpointLocation)\
                .option("path", OUTPUT)\
                .option("compression", 'snappy')\
            .trigger(processingTime='10 seconds')\
            .start()

        query.awaitTermination(60)
        print("""
        done saving the stream!
        Check the %s""" %(OUTPUT))









if __name__ == "__main__":


    spark = SparkSession.builder \
        .enableHiveSupport() \
        .appName("SimpleApp") \
        .getOrCreate()

    from queries import Queries


    # spark = spark

    pipeline = BasicPipeline(INPUT, spark)
    df = pipeline.transform()

    print("first 4 rows of the transformed dataset")
    print(df.show(4, truncate=False))

    pipeline.save()

    print("the first line of the saved Parquet:\n")
    print(spark.read.parquet(OUTPUT).head())



    queries =  Queries(df)

    print(50*"X")
    print("the most sold products")
    print(50*'X')
    queries.most_sold_products_pyspark()
    queries.most_sold_products_sql()

    print(50*"X")
    print("day for the highest revenue")
    print(50*'X')
    res = queries.highest_revenue_date_pyspark()
    print(res.take(1)[0].date)
    res.show(5)
    queries.highest_revenue_date_sql()

    print(50 * "X")
    print("top 5 selling products for each day ")
    print(50 * 'X')
    queries.top_5_selling_each_day_pyspark()
    queries.top_5_selling_each_day_sql()



    # stream = StreamPipeline(STREAM_DATA, spark)
    # df = stream.transform()
    # stream.save()
