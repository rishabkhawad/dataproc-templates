# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Dict, Sequence, Optional, Any
from logging import Logger
import argparse
import pprint

from pyspark.sql import SparkSession

from dataproc_templates import BaseTemplate
from dataproc_templates.util.argument_parsing import add_spark_options
from dataproc_templates.util.dataframe_reader_wrappers import ingest_dataframe_from_cloud_storage
import dataproc_templates.util.template_constants as constants


__all__ = ['BigqueryToBigQueryTemplate']


class BigqueryToBigQueryTemplate(BaseTemplate):
    """
    Dataproc template implementing loads from GCS into BigQuery
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.BQ_BQ_INPUT_DATASET}',
            dest=constants.BQ_BQ_INPUT_DATASET,
            required=True,
            help='BigQuery dataset for the input table'
        )


        parser.add_argument(
            f'--{constants.BQ_BQ_INPUT_TABLE}',
            dest=constants.BQ_BQ_INPUT_TABLE,
            required=True,
            help='BigQuery input table name'
        )

        parser.add_argument(
            f'--{constants.BQ_BQ_OUTPUT_DATASET}',
            dest=constants.BQ_BQ_OUTPUT_DATASET,
            required=True,
            help='BigQuery dataset for the output table'
        )


        parser.add_argument(
            f'--{constants.BQ_BQ_OUTPUT_TABLE}',
            dest=constants.BQ_BQ_OUTPUT_TABLE,
            required=True,
            help='BigQuery output table name'
        )


        # parser.add_argument(
        #     f'--{constants.BQ_BQ_INPUT_FORMAT}',
        #     dest=constants.BQ_BQ_INPUT_FORMAT,
        #     required=True,
        #     help='Input file format (one of: avro,parquet,csv,json,delta)',
        #     choices=[
        #         constants.FORMAT_AVRO,
        #         constants.FORMAT_PRQT,
        #         constants.FORMAT_CSV,
        #         constants.FORMAT_JSON,
        #         constants.FORMAT_DELTA
        #     ]
        # )

        add_spark_options(parser, constants.get_csv_input_spark_options("gcs.bigquery.input."))


        parser.add_argument(
            f'--{constants.BQ_BQ_OUTPUT_MODE}',
            dest=constants.BQ_BQ_OUTPUT_MODE,
            required=False,
            default=constants.OUTPUT_MODE_APPEND,
            help=(
                'Output write mode '
                '(one of: append,overwrite,ignore,errorifexists) '
                '(Defaults to append)'
            ),
            choices=[
                constants.OUTPUT_MODE_OVERWRITE,
                constants.OUTPUT_MODE_APPEND,
                constants.OUTPUT_MODE_IGNORE,
                constants.OUTPUT_MODE_ERRORIFEXISTS
            ]
        )

        parser.add_argument(
            f'--{constants.BQ_BQ_LD_TEMP_BUCKET_NAME}',
            dest=constants.BQ_BQ_LD_TEMP_BUCKET_NAME,
            required=True,
            help='Cloud Storage location for staging, Format: <bucket-name>'
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        input_dataset: str = args[constants.BQ_BQ_INPUT_DATASET]
        input_table: str = args[constants.BQ_BQ_INPUT_TABLE]
        big_query_dataset: str = args[constants.BQ_BQ_OUTPUT_DATASET]
        big_query_table: str = args[constants.BQ_BQ_OUTPUT_TABLE]
        bq_temp_bucket: str = args[constants.BQ_BQ_LD_TEMP_BUCKET_NAME]
        output_mode: str = args[constants.BQ_BQ_OUTPUT_MODE]
        logger.info(
            "Starting BigQuery to BigQuery Spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )

        # Read
        input_data = spark.read \
            .format(constants.FORMAT_BIGQUERY) \
            .option(constants.TABLE, input_dataset + "." + input_table) \
            .load()
        sql_query = True
        # Transformation 

        if True or sql_query:
            # Create temp view on source data
            input_data.createOrReplaceTempView("demo")
            # Execute SQL
            output_data = spark.sql("SELECT *, CONCAT(name, ' is ', age, ' years old') AS concat_col FROM demo")
            # output_data = spark.sql(sql_query)
        else:
            output_data = input_data

        # Write
        output_data.write \
            .format(constants.FORMAT_BIGQUERY) \
            .option(constants.TABLE, big_query_dataset + "." + big_query_table) \
            .option(constants.TEMP_GCS_BUCKET, bq_temp_bucket) \
            .mode(output_mode) \
            .save()
