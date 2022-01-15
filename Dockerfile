FROM python:3.7

WORKDIR /mysqldb
COPY . /mysqldb

RUN apt-get update && apt-get install -y default-mysql-client

RUN pip install -r requirements.txt

# CMD ["python", "main.py"]