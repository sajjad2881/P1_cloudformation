import json
import boto3
import os
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

s3 = boto3.client('s3')
rekognition = boto3.client('rekognition')

def lambda_handler(event, context):
    # Get the S3 bucket and key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    print(event)
    # Call the Rekognition detectLabels method to detect labels in the image
    responseRekognition = rekognition.detect_labels(
        Image={
            'S3Object': {
                'Bucket': bucket,
                'Name': key
            }
        }
    )
    
    # use the S3 client's headObject method to retrieve object metadata
    responseS3 = s3.head_object(Bucket=bucket, Key=key)
    customLabels = None
    
    # retrieve the customLabels metadata field if it exists
    metadata = responseS3["Metadata"]
    print(metadata)

    if "customlabels" in  metadata:
        customLabels = metadata["customlabels"].split(",")
        print(customLabels)
    
    timeStamp = responseS3['LastModified']
    createdAtTimestamp = timeStamp.strftime("%Y-%m-%dT%H:%M:%S")


    # Print the detected labels
    labels = [label['Name'] for label in responseRekognition['Labels']]
    
    if customLabels:
        labels.extend(customLabels)
    
    labels = [l.lower() for l in labels]

    print(labels)

    indexData = {
        
        "objectKey" : key,
        "bucket": bucket,
        "createdAtTimestamp": createdAtTimestamp,
        "labels": labels
    }
    
    print(indexData)
    
    opensearch_client = get_opensearch_client()
    response = opensearch_client.index(index="photos", body=indexData)
    print(json.dumps(response))
    
    return {
        "statusCode": 200,
        "body": json.dumps(response)
    }
    
    
    
    
def get_opensearch_client():
    session = boto3.Session()
    region = session.region_name
    host = os.environ["OPENSEARCH_ENDPOINT"]
    print("host was ", host)
    host = str(host) + "/"
    print("host is ", host)
    
    service = "es"
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

    return Elasticsearch(
    hosts=[host],
    port=443,
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    )
