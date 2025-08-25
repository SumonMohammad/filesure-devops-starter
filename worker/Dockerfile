FROM python:3.9
WORKDIR /app
COPY worker/ ./worker/
COPY requirements.txt .
RUN pip install -r requirements.txt
EXPOSE 9100
CMD ["python", "worker/downloader.py"]