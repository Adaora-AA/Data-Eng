FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 requests pyarrow fastparquet

WORKDIR /app
COPY ingest_homework.py ingest_homework.py

ENTRYPOINT ["python", "ingest_homework.py"]