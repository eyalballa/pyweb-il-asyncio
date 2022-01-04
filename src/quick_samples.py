import boto3

from sync_classes.sync_db_connecton import db_connection


def read_from_s3():
    s3 = boto3.client('s3')
    bucket = 'bucket'
    filename = 'filename'
    data = s3.get_object(Bucket=bucket, Key=filename)
    contents = data['Body'].read()
    print(contents.decode("utf-8"))


def query_pg():
    uri = 'db_uri'
    query = 'select * from TABLE'
    with db_connection(uri) as conn:
        items = conn.execute_and_get_all(query)
        for item in items:
            print(item)


def main():
    read_from_s3()
    query_pg()


if __name__ == '__main__':
    main()