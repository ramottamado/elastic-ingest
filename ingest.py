from elasticsearch import helpers, Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import pandas as pd
import boto3
import json

gen_df = 'YOUR JSON FILE HERE'
index = 'YOUR ELASTIC INDEX'

host = 'YOUR HOST'
region = 'YOUR AWS REGION'
service = 'YOUR AWS SERVICE'
credentials = boto3.Session(
    aws_access_key_id='YOUR AWS ACCES KEY',
    aws_secret_access_key='YOUR AWS SECRET KEY'
).get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service)

es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)


def gendata():
    _id = 0
    with open(gen_df) as df:
        for lines in df:
            _id += 1
            try:
                yield {
                    "_index": index,
                    "_type": "_doc",
                    "_id": _id,
                    "_source": eval(lines),
                }
            except Exception as e:
                print(str(e))


helpers.bulk(es, gendata())
