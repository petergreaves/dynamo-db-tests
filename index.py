from boto3 import resource
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime

co_table = resource('dynamodb').Table('customer-orders')

def insert():
    print('Inserting some data...')
    response = co_table.put_item(
        Item = {
            "customer_id": "4",
            "order_id": "AD332",
            "status": "pending",
            "order_date": datetime.now().isoformat()
        })
    print('Response : %s', response)


def select_scan():
    print(f'select scan demo')
    filter_expression = Attr('status').eq('pending')
    
    item_list = []
    dynamo_response = {'LastEvaluatedKey': False}
    while 'LastEvaluatedKey' in dynamo_response:
        if dynamo_response['LastEvaluatedKey']:
            dynamo_response = co_table.scan(
                FilterExpression=filter_expression,
                ExclusiveStartKey=dynamo_response['LastEvaluatedKey']
            )
            print(f'response-if: {dynamo_response}' )
        else:
            dynamo_response = co_table.scan(
                FilterExpression=filter_expression)
            print(f'response-if: {dynamo_response}' )

    for i in dynamo_response['Items']:
        item_list.append(i)
    
    print(f'Number of tasks to process: {len(item_list)}')
    for item in item_list:
        print(f'Item {item}')


def query_by_partition_key(customer_id):
    print(f'query by key demo')
    filter_expression = Key('customer_id').eq(customer_id)

    response = co_table.query(
        KeyConditionExpression=filter_expression
        )
    for item in response["Items"]:
            print(item)

def query_by_partition_key_ordered(customer_id):
    print(f'query by key demo')
    filter_expression = Key('customer_id').eq(customer_id)

    response = co_table.query(
        KeyConditionExpression=filter_expression,
        ScanIndexForward=False
        )
    for item in response["Items"]:
            print(item)



#query_by_partition_key('1')
query_by_partition_key_ordered('1')
#insert()
#select_scan()