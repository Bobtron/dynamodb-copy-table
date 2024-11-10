FROM python:3.13-alpine

LABEL maintainer="github.com/Bobtron"
LABEL org.opencontainers.image.authors="fiodel"
LABEL org.opencontainers.image.title="DynamoDB Copy Table"
LABEL org.opencontainers.image.url="https://github.com/Bobtron/dynamodb-copy-table"
LABEL org.opencontainers.image.documentation="https://github.com/Bobtron/dynamodb-copy-table#readme"
LABEL org.opencontainers.image.source="https://github.com/Bobtron/dynamodb-copy-table"

WORKDIR /project
COPY . /project
RUN pip install -r requirements.txt
ENTRYPOINT ["python","dynamodb-copy-table.py"]
