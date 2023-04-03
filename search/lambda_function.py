
import json
import logging
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import inflection


logger = logging.getLogger()
logger.setLevel(logging.INFO)
client = boto3.client('lexv2-runtime')
# Configure OpenSearch connection!!!!!!! Test comment lol
region = 'us-east-1' # Replace with your OpenSearch region
service = 'es'
credentials = boto3.Session().get_credentials()
aws_auth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

opensearch = OpenSearch(
    hosts=[{'host': 'search-photos-bpp72xnr3jxv4jyyvb753ahzty.us-east-1.es.amazonaws.com', 'port': 443}],
    http_auth=aws_auth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

def search_photos(query):
    q = query[0].replace("%20", " ")

    # ... (keep your existing code) ...
    response = client.recognize_text(
        botAliasId= "KB7Z6WZUVV",
        botId="PDA8MVXRQM",
        sessionId='testuser',
        localeId="en_US",
        text=q
    )
    
    query1 = response["sessionState"]["intent"]["slots"]["query1"]
    query2 = response["sessionState"]["intent"]["slots"]["query2"]
    
    print(response)
    
    if query1:
        query1 = response["sessionState"]["intent"]["slots"]["query1"]["value"]["interpretedValue"]
        query1 = inflection.singularize(query1) 
        logger.info('At Query1')
        
    if query2:
        query2 = response["sessionState"]["intent"]["slots"]["query2"]["value"]["interpretedValue"]
        query2 = inflection.singularize(query2) 
        logger.info('At Query1')
        
    print(query1)
    print(query2)
    
    # Perform the search in the OpenSearch "photos" index
    should_clauses = []
    if query1:
        should_clauses.append({"match": {"labels": query1}})
    if query2:
        should_clauses.append({"match": {"labels": query2}})

    search_body = {
        "query": {
            "bool": {
                "should": should_clauses
            }
        }
    }
    
    search_response = opensearch.search(index="photos", body=search_body)
    logger.info(f'The search_response is: {json.dumps(search_response)}')

    # Generate pre-signed URLs and format the results
    s3_client = boto3.client('s3')
    search_results = []
    prioritized_results = []
    alr_seen = []

    for hit in search_response['hits']['hits']:
        source = hit['_source']
        if source['objectKey'] not in alr_seen:
            source = hit['_source']
            object_key = source['objectKey']
            bucket = source['bucket']
            labels = source['labels']
    
            presigned_url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket, 'Key': object_key},
                ExpiresIn=3600
            )
    
            result = {
                "url": presigned_url,
                "labels": labels
            }
    
            if query1 and query2 and query1 in labels and query2 in labels:
                prioritized_results.append(result)
            else:
                search_results.append(result)
                
            alr_seen.append(object_key)

    # Combine prioritized and other results
    search_results = prioritized_results + search_results
    
    logger.info(f'Returning: {json.dumps(search_results)}')
    return search_results


def lambda_handler(event, context):
    # Log the incoming event
    logger.info(f'Event: {json.dumps(event)}')
    
    print(event)

    # Check if the request is a preflight (OPTIONS) request
    if event.get('httpMethod') == 'OPTIONS':
        response = {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type, x-amz-meta-customLabels',
                'Access-Control-Allow-Methods': 'GET'
            }
        }
    else:
        path = event.get('path')
        logger.info(f'Path: {json.dumps(path)}')
        if path == "/search":
            query = event['multiValueQueryStringParameters']['q']
            search_results = search_photos(query)
            response = {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Content-Type': 'application/json'
                },
                'body': json.dumps({
                    'results': search_results
                })
            }
        else:
            response = {
                'statusCode': 404,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Content-Type': 'application/json'
                },
                'body': json.dumps({
                    'code': 404,
                    'message': 'Not found'
                })
            }

    # Log the response object
    logger.info(f'Response: {json.dumps(response)}')

    # Return the response
    return response
