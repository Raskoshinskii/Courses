FROM python:3.8.0

RUN apt-get install wget
RUN pip install pandas numpy sqlalchemy psycopg2 pyarrow

WORKDIR /app

COPY run_injesting_pipeline.py run_injesting_pipeline.py

ENTRYPOINT [ "python", "run_injesting_pipeline.py" ]