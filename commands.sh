gcloud dataproc jobs submit pyspark \
    --cluster=de-flight-cluster \
    --region=europe-west2 \
    --jars=gs://spark-lib/bigquery/spark-3.4-bigquery-0.37.0.jar \
    gs://de_zoomcamp_flight_data/code/spark_transformer.py