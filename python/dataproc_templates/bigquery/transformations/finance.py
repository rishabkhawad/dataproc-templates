from dataproc_templates.bigquery.bigquery_to_bigquery import BigqueryToBigQueryTemplate
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

class Finance(BigqueryToBigQueryTemplate):
    def execute(self, spark: SparkSession, df: DataFrame):
        return df.withColumn("uuid", f.expr("uuid()"))