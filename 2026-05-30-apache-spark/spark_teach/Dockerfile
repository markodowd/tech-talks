FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYSPARK_PYTHON=python3

RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    jupyterlab==4.3.6

COPY notebook.ipynb /app/notebook.ipynb

WORKDIR /app
EXPOSE 8890

CMD ["jupyter", "lab", \
     "--ip=0.0.0.0", "--port=8890", \
     "--no-browser", "--allow-root", \
     "--NotebookApp.token=''", "--NotebookApp.password=''"]
