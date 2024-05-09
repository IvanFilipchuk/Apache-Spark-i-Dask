FROM apache/spark:latest
COPY wordcount.py /
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[*]", "/wordcount.py"]
CMD []