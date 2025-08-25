FROM python:3.9-slim
WORKDIR /app
COPY api/app.py /app/
COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt
EXPOSE 5001
CMD ["python", "app.py"]