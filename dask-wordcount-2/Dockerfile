FROM python:latest
RUN pip install dask[complete]
COPY wordcount.py /
ENTRYPOINT ["python", "/wordcount.py"]
CMD []