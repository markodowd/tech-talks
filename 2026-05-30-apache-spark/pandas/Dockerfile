FROM python:3.11-slim

RUN pip install --no-cache-dir \
    pandas==2.2.3 \
    pyarrow==17.0.0 \
    matplotlib==3.9.4 \
    jupyterlab==4.3.6

COPY process.py   /app/process.py
COPY notebook.ipynb /app/notebook.ipynb

WORKDIR /app
EXPOSE 8888

CMD ["jupyter", "lab", \
     "--ip=0.0.0.0", "--port=8888", \
     "--no-browser", "--allow-root", \
     "--NotebookApp.token=''", "--NotebookApp.password=''"]
