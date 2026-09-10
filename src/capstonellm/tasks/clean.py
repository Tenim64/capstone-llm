import argparse
import logging

import boto3
import pyspark.sql.functions as sf
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from capstonellm.common.catalog import llm_bucket
from capstonellm.common.spark import ClosableSparkSession

logger = logging.getLogger(__name__)

RawOwnerSchema = StructType([
    StructField("account_id", IntegerType(), True),
    StructField("reputation", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("user_type", StringType(), True),
    StructField("accept_rate", IntegerType(), True),
    StructField("profile_image", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("link", StringType(), True),
])

RawQuestionStructure = StructType([
    StructField("accepted_answer_id", StringType(), True),
    StructField("answer_count", StringType(), True),
    StructField("body", StringType(), True),
    StructField("closed_date", TimestampType(), True),
    StructField("closed_reason", StringType(), True),
    StructField("content_license", StringType(), True),
    StructField("creation_date", TimestampType(), True),
    StructField("is_answered", BooleanType(), True),
    StructField("last_activity_date", TimestampType(), True),
    StructField("last_edit_date", TimestampType(), True),
    StructField("link", StringType(), True),
    StructField("owner", RawOwnerSchema, True),
    StructField("protected_date", StringType(), True),
    StructField("question_id", IntegerType(), True),
    StructField("score", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("title", StringType(), True),
    StructField("view_count", IntegerType(), True),
])

RawResponseStructure = StructType([
    StructField("owner", RawOwnerSchema, True),
    StructField("is_accepted", BooleanType(), True),
    StructField("score", IntegerType(), True),
    StructField("last_activity_date", TimestampType(), True),
    StructField("last_edit_date", TimestampType(), True),
    StructField("creation_date", TimestampType(), True),
    StructField("answer_id", IntegerType(), True),
    StructField("question_id", IntegerType(), True),
    StructField("content_license", StringType(), True),
    StructField("body", StringType(), True),
])

RawQuestionJsonStructure = StructType([
    StructField("items", ArrayType(RawQuestionStructure), True)
])

RawResponseJsonStructure = StructType([
    StructField("items", ArrayType(RawResponseStructure), True)
])

ResponseStructure = StructType([
    StructField("title", StringType(), True),
    StructField("response_body", StringType(), True),
    StructField("response_score", StringType(), True),
    StructField("is_accepted_response", StringType(), True),
])

QuestionStructure = StructType([
    StructField("question_id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("body", StringType(), True),
    StructField("score", StringType(), True),
    StructField("is_answered", StringType(), True),
    StructField("responses", ArrayType(ResponseStructure), True),
])

BUCKET_NAME = "dataminded-academy-capstone-llm-data"

logger = logging.getLogger(__name__)

def fetchTechnologyFilemap(tag=None):
    # Structure:
    # input/
    #.  - <technology>/
    #.      - answers.json
    #.      - questions.json
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    prefix = f"input/{tag}/" if tag else "input"

    pairs_by_tech = {}

    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            technology = key.split("/")[1]

            pair = pairs_by_tech.setdefault(technology, {"technology": technology, "questions": None, "answers": None})

            if key.endswith("questions.json"):
                pair["questions"] = key
            elif key.endswith("answers.json"):
                pair["answers"] = key

    return pairs_by_tech

def push(results_table, tag: str):
    output_path = f"s3a://{BUCKET_NAME}/cleaned/Tenim64/{tag}"
    logger.info(f"Writing cleaned question documents to {output_path}")
    results_table.write.mode("overwrite").json(output_path)

def clean(spark: SparkSession, environment: str, tag: str):
    files_map = fetchTechnologyFilemap(tag)
    questions_table = spark.createDataFrame([], RawQuestionStructure)
    answers_table = spark.createDataFrame([], RawResponseStructure)

    for technology, paths in files_map.items():
        if tag and technology != tag:
            continue

        if paths["questions"] is not None:
            questions_path = f"s3a://{BUCKET_NAME}/{paths['questions']}"
            questions_json_raw = spark.read.schema(RawQuestionJsonStructure).json(questions_path)
            questions_df_raw = questions_json_raw.select(sf.explode(sf.col("items")).alias("question")).select("question.*")
            logger.info(f"Technology '{technology}' lists {questions_df_raw.count()} questions")
            questions_table = questions_table.union(questions_df_raw)

        if paths["answers"] is not None:
            answers_path = f"s3a://{BUCKET_NAME}/{paths['answers']}"
            answers_json_raw = spark.read.schema(RawResponseJsonStructure).json(answers_path)
            answers_df_raw = answers_json_raw.select(sf.explode(sf.col("items")).alias("answer")).select("answer.*")
            logger.info(f"Technology '{technology}' lists {answers_df_raw.count()} answers")
            answers_table = answers_table.union(answers_df_raw)

    # Given the input data for 1 tag, the goal is to:
    # - create 1 json document per question containing:
    #   - the title, question body and the response body.
    # So your goal is to extract the relevant fields from both the questions and answers
    # and join them together using the question_id field.

    logger.info(f"Found {questions_table.count()} questions and {answers_table.count()} answers")
    logger.info(f"Columns questions table include: {questions_table.columns}")
    logger.info(f"Columns answers table include: {answers_table.columns}")

    results_table = (
            questions_table
            .select("question_id", "title", "body", "score", "is_answered")
            .join(
                answers_table
                .filter(
                    (sf.col("score") >= 0) | sf.col("is_accepted")
                )
                .select(
                    sf.col("question_id"),
                    sf.col("body").alias("response_body"),
                    sf.col("score").alias("response_score"),
                    sf.col("is_accepted").alias("is_accepted_response")
                ),
                on="question_id",
                how="left"
            )
            .groupBy(
                "question_id"
            )
            .agg(
                sf.first("title").alias("title"),
                sf.first("body").alias("body"),
                sf.first("score").alias("score"),
                sf.first("is_answered").alias("is_answered"),
                sf.collect_list(
                    sf.when(
                        sf.col("response_body").isNotNull(),
                        sf.struct("response_body", "response_score", "is_accepted_response")
                    )
                ).alias("responses")
            )
        )

    logger.info(f"Results table has {results_table.count()} columns: {results_table.columns}")

    push(results_table, tag)

def main():
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=False, default="local"
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="python-polars", required=False
    )
    logger.info("starting the cleaning job")

    args = parser.parse_args()
    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
    }
    if args.env == "local":
        print("This is a local execution of the capestonellm project")
        builder = SparkSession.builder.appName("Spark S3 Integration").config(
            "spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2"
        )
        for key, value in common_spark_config.items():
            builder = builder.config(key, value)
        session = builder.getOrCreate()
        clean(session, args.env, args.tag)
    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            clean(session, args.env, args.tag)


if __name__ == "__main__":
    main()
