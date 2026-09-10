import argparse
import logging
from typing import List

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

logger = logging.getLogger(__name__)
spark = SparkSession.builder.getOrCreate()

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
    StructField("question_id", StringType(), True),
    StructField("score", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("title", StringType(), True),
    StructField("view_count", IntegerType(), True),
])

RawQuestionJsonStructure = StructType([
    StructField("items", ArrayType(RawQuestionStructure), True)
])

BUCKET_NAME = "dataminded-academy-capstone-llm-data"
DEBUG_MODE = True

def debugPrint(text):
    if DEBUG_MODE:
        print(text)

def fetchTechnologyFilemap():
    # Structure:
    # input/
    #.  - <technology>/
    #.      - answers.json
    #.      - questions.json
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    pairs_by_tech = {}

    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix="input"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            technology = key.split("/")[1]

            pair = pairs_by_tech.setdefault(technology, {"technology": technology, "questions": None, "answers": None})

            if key.endswith("questions.json"):
                pair["questions"] = key
            elif key.endswith("answers.json"):
                pair["answers"] = key

    return pairs_by_tech

def ingest(tag: str):
    files_map = fetchTechnologyFilemap()
    questions_table = spark.createDataFrame([], RawQuestionStructure)
    # answers_table = spark.createDataFrame([], RawAnswerStructure)

    for technology, paths in files_map.items():
        if tag and technology != tag:
            continue

        if paths["questions"] is not None:
            questions_path = f"s3://{BUCKET_NAME}/{paths['questions']}"
            questions_json_raw = spark.read.schema(RawQuestionJsonStructure).json("questions.json")
            questions_df_raw = questions_json_raw.select(sf.explode(sf.col("items")).alias("question")).select("question.*")
            debugPrint(f"Technology '{technology}' lists {questions_df_raw.count()} questions")
            questions_table = questions_table.union(questions_df_raw)

        if paths["answers"] is not None:
            answers_path = f"s3://{BUCKET_NAME}/{paths['answers']}"
            # answers_json_raw = spark.read.schema(RawAnswerJsonStructure).json("answers.json")
            # answers_df_raw = json_raw.select(sf.explode(sf.col("items")).alias("answer")).select("answer.*")
            # answers_table = answers_table.union(answers_df_raw)
            # debugPrint(f"Technology '{technology}' lists {answers_df_raw.count()} answers")
    # debugPrint(f"Found {questions_table.count()} questions and {answers_table.count()} answers")
    debugPrint(f"Found {questions_table.count()} questions")
    

def main():
    parser = argparse.ArgumentParser(description="stackoverflow ingest")
    parser.add_argument(
        "-t", "--tag", dest="tag", help="Tag of the question in stackoverflow to process",
        default="python-polars", required=False
    )
    args = parser.parse_args()
    logger.info("Starting the ingest job")

    ingest(args.tag)


if __name__ == "__main__":
    main()
