FROM public.ecr.aws/dataminded/spark-k8s-glue:v4.0.1-hadoop-3.4.2-v4

USER 0
ENV PYSPARK_PYTHON python3
WORKDIR /opt/spark/work-dir

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml requirements.txt README.md ./
COPY src/ ./src/

RUN uv pip install --system --no-cache -r requirements.txt

ENTRYPOINT ["python3", "-m"]
