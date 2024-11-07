FROM python:3.12-alpine
WORKDIR /project
COPY . /project
RUN pip install -r requirements.txt
ENTRYPOINT ["python","dynamodb-copy-table.py"]
