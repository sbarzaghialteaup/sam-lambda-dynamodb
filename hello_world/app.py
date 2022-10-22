import json

# import requests

import boto3
import json
from botocore.exceptions import ClientError

ERROR_HELP_STRINGS = {
    # Common Errors
    'InternalServerError': 'Internal Server Error, generally safe to retry with exponential back-off',
    'ProvisionedThroughputExceededException': 'Request rate is too high. If you\'re using a custom retry strategy make sure to retry with exponential back-off.' +
                                              'Otherwise consider reducing frequency of requests or increasing provisioned capacity for your table or secondary index',
    'ResourceNotFoundException': 'One of the tables was not found, verify table exists before retrying',
    'ServiceUnavailable': 'Had trouble reaching DynamoDB. generally safe to retry with exponential back-off',
    'ThrottlingException': 'Request denied due to throttling, generally safe to retry with exponential back-off',
    'UnrecognizedClientException': 'The request signature is incorrect most likely due to an invalid AWS access key ID or secret key, fix before retrying',
    'ValidationException': 'The input fails to satisfy the constraints specified by DynamoDB, fix input before retrying',
    'RequestLimitExceeded': 'Throughput exceeds the current throughput limit for your account, increase account level throughput before retrying',
}

# Use the following function instead when using DynamoDB Local
def create_dynamodb_client():
   return boto3.client("dynamodb", region_name="localhost", endpoint_url="http://172.17.0.2:8000")

# def create_dynamodb_client(region="us-east-1"):
#     return boto3.client("dynamodb", region_name=region)


def create_query_input():
    return {
        "TableName": "Movies",
        "KeyConditionExpression": "#4ff00 = :4ff00",
        "ExpressionAttributeNames": {"#4ff00":"year"},
        "ExpressionAttributeValues": {":4ff00": {"N":"2000"}},
        # "KeyConditionExpression": "#d3cc0 = :d3cc0 And #d3cc1 = :d3cc1",
        # "ExpressionAttributeNames": {"#d3cc0":"year","#d3cc1":"title"},
        # "ExpressionAttributeValues": {":d3cc0": {"N":"2004"},":d3cc1": {"S":"2046"}},
        "ReturnConsumedCapacity": "INDEXES"
    }


def execute_query(dynamodb_client, input):
    try:
        response = dynamodb_client.query(**input)
        # print("Query successful.")
        # Handle response
    except ClientError as error:
        handle_error(error)
    except BaseException as error:
        print("Unknown error while querying: " + error.response['Error']['Message'])

    # print(json.dumps(response['Items'][0]))
    print(json.dumps(response))
    return response

def handle_error(error):
    error_code = error.response['Error']['Code']
    error_message = error.response['Error']['Message']

    error_help_string = ERROR_HELP_STRINGS[error_code]

    print('[{error_code}] {help_string}. Error message: {error_message}'
          .format(error_code=error_code,
                  help_string=error_help_string,
                  error_message=error_message))


def main():
    # Create the DynamoDB Client with the region you want
    dynamodb_client = create_dynamodb_client()

    # Create the dictionary containing arguments for query call
    query_input = create_query_input()

    # Call DynamoDB's query API
    return execute_query(dynamodb_client, query_input)


def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    # try:
    #     ip = requests.get("http://checkip.amazonaws.com/")
    # except requests.RequestException as e:
    #     # Send some context about this error to Lambda Logs
    #     print(e)

    #     raise e

    response = main()

    return {
        "statusCode": 200,
        "body": json.dumps(response),
    }
