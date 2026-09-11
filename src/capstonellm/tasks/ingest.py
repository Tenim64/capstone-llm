import argparse
import json
import logging
import time
from typing import Dict, List, Optional

import boto3
import requests

logger = logging.getLogger(__name__)

API_BASE_URL = "https://api.stackexchange.com/2.3"
SITE = "stackoverflow"
PAGE_SIZE = 100
MAX_PAGES = 25
ANSWER_CHUNK_SIZE = 100
REQUEST_TIMEOUT = 30

BUCKET_NAME = "dataminded-academy-capstone-llm-data"
# Personal namespace so raw dumps don't clash with the data provided by others,
# mirrors the handle already used by the cleaning script's `push()` output path.
USER = "Tenim64"


def _call_api(endpoint: str, params: dict) -> list[dict]:
    """Call a Stack Exchange API endpoint, following pagination and backoff until exhausted."""
    items: list[dict] = []
    page = 1
    base_params = {
        "site": SITE,
        "pagesize": PAGE_SIZE,
        **params,
    }

    while True:
        response = requests.get(
            f"{API_BASE_URL}/{endpoint}",
            params={**base_params, "page": page},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        items.extend(payload.get("items", []))

        backoff = payload.get("backoff")
        if backoff:
            logger.info(f"API requested a backoff of {backoff}s, sleeping")
            time.sleep(backoff)

        quota_remaining = payload.get("quota_remaining")
        if quota_remaining is not None and quota_remaining < 5:
            logger.warning(f"Stopping early, quota nearly exhausted ({quota_remaining} left)")
            break

        if not payload.get("has_more") or page >= MAX_PAGES:
            break
        page += 1

    return items


def fetch_questions(tag: str) -> list[dict]:
    logger.info(f"Fetching questions tagged '{tag}'")
    questions = _call_api(
        "questions",
        {
            "tagged": tag,
            "filter": "withbody",
            "sort": "votes",
            "order": "desc",
        },
    )
    logger.info(f"Fetched {len(questions)} questions for tag '{tag}'")
    return questions


def fetch_answers(question_ids: list[int]) -> list[dict]:
    answers: list[dict] = []
    for i in range(0, len(question_ids), ANSWER_CHUNK_SIZE):
        chunk = question_ids[i : i + ANSWER_CHUNK_SIZE]
        ids_param = ";".join(str(question_id) for question_id in chunk)
        logger.info(f"Fetching answers for {len(chunk)} questions")
        answers.extend(
            _call_api(
                f"questions/{ids_param}/answers",
                {"filter": "withbody"},
            )
        )
    logger.info(f"Fetched {len(answers)} answers")
    return answers


def push_to_s3(items: list[dict], tag: str, filename: str, s3_client=None):
    s3_client = s3_client or boto3.client("s3")
    key = f"input/{USER}/{tag}/{filename}"
    logger.info(f"Writing {len(items)} raw items to s3://{BUCKET_NAME}/{key}")
    body = json.dumps({"items": items}).encode("utf-8")
    s3_client.put_object(Bucket=BUCKET_NAME, Key=key, Body=body)


def ingest(tag: str):
    questions = fetch_questions(tag)
    push_to_s3(questions, tag, "questions.json")

    question_ids = [q["question_id"] for q in questions if "question_id" in q]
    answers = fetch_answers(question_ids)
    push_to_s3(answers, tag, "answers.json")


def main():
    logging.basicConfig(level=logging.INFO)
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
