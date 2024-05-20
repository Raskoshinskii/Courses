FROM python:3.10

RUN pip install pandas numpy

WORKDIR /simple_ml_app

COPY simple_pipeline.py simple_pipeline.py

ENTRYPOINT [ "python", "simple_pipeline.py" ]