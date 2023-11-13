# import datetime
import json
import os
import subprocess
# import time
# from datetime import date
from typing import List, Dict
from datetime import datetime, timedelta, timezone
from boto3.dynamodb.conditions import Key
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from zoneinfo import ZoneInfo


import boto3
load_dotenv()
host = os.environ["CORE_HOST"]
user = os.environ["CORE_USER"]
pwd = os.environ["CORE_PW"]
port = os.environ["CORE_PORT"]
db = os.environ["CORE_DATABASE"]




def refreshSecurityToken():
    p = subprocess.Popen(['okta-awscli', '--profile', 'core', '--okta-profile', 'core'])
    print(p.communicate())

refreshSecurityToken()



def dynamo(cmp_id:str,Start_date:str,end_date:str) -> List[Dict[str, any]]:
    retArr = []
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    # table = dynamodb.Table('rtb-beeswax-spend-aggregates-prod')
    table = dynamodb.Table('BeeswaxCampaignSpendByDeviceTypeAggregates-prod')
    newJson = {}
    date = Start_date.split(" ")[0].replace("-","")
    t1 =  int(Start_date.split(" ")[1].split(":")[0])
    t2 = int(end_date.split(" ")[1].split(":")[0])
    for i in range(t1, t2):
        hour_str = str(i).zfill(2)
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('CampaignId_TimeBucket').eq(
                f'{cmp_id}#{date}{hour_str}')
        )
        items = response['Items']

        for data in items:
            newJson[f'{data["CampaignId_TimeBucket"]}_{data["DeviceType"]}'] =  int(data["TotalSpendMicros"])
    return newJson

# BeeswaxCampaignSpendByDeviceTypeRecords-prod



    # KeyConditionExpression=boto3.dynamodb.conditions.K  ey('ExpirationTime')

        # items = response['Items']

    #     for data in items:
    #         newJson[f'{data["CampaignId_TimeBucket"]}'] = int(data["CreationTime"])
    #         # newJson[f'{data["CampaignId_TimeBucket"]}_{data["DeviceType"]}'] =  int(data["TotalSpendMicros"])
    # return retArr

def dynamoRecords(cmp_id:str,Start_date:str,end_date:str) -> List[Dict[str, any]]:
    retArr = []
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('rtb-beeswax-spend-records-prod')
    # table = dynamodb.Table('rtb-beeswax-spend-records-prod')
    newJson = {}
    date = Start_date.split(" ")[0].replace("-","")
    t1 =  int(Start_date.split(" ")[1].split(":")[0])
    t2 = int(end_date.split(" ")[1].split(":")[0])
    for i in range(t1, t2):
        hour_str = str(i).zfill(2)
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('CampaignId_TimeBucket').eq(
                f'{cmp_id}#{date}{hour_str}')
        )
        items = response['Items']

        for data in items:
            newJson[f'{data["CampaignId_TimeBucket"]}_{data["DeviceType"]}'] =  int(data["TotalSpendMicros"])
    return newJson



def run_query_and_collect_results(start_date:str,end_date:str,camp_id:int) -> dict:
    database_url = "postgresql://{0}:{1}@{2}:{3}/{4}".format(user, pwd, host, port, db)
    engine = create_engine(database_url)
    query = text(f"""
        SELECT c.platform_device_type,
               HOUR(c.imp_rx_time_utc) AS bid_hour,
               c.line_item_id AS camp_id,
               (SUM(c.win_cost_micros_usd)) AS hourly_spend
        FROM win_logs c
        WHERE c.time >= '{start_date}'::timestamp - interval '1' hour
              AND c.time < '{end_date}'::timestamp + interval '1' hour
              AND c.imp_rx_time_utc >= '{start_date}'
              AND c.imp_rx_time_utc < '{end_date}'
              AND c.line_item_id = {camp_id}
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """)
    results_dict = {}
    with engine.connect() as connection:
        result = connection.execute(query)
        for row in result:
            # results_dict = {}
            time = row[1].strftime('%Y%m%d%H')
            results_dict[f'{row[2]}#{time}_{row[0]}'] = int(row[3])
    return results_dict

def runner(startDate:str,endDate:str,cmpId:int):
    start = datetime.strptime(startDate, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("America/Los_Angeles")).astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S")
    end = datetime.strptime(endDate, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("America/Los_Angeles")).astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S")
    print(f'test results for the {cmpId} for the dates {start} to {end}')
    print(f'Start Date:- {startDate}, Start UTC:-{start} and End Date:-{endDate} and End UTC:-{end}')
    queryResults = run_query_and_collect_results(startDate,endDate,cmpId)
    dynamoDbResults = dynamo(cmpId,startDate,endDate)
    dynamoMissing = set(queryResults.keys()) -set(dynamoDbResults.keys())
    dbMissing = set(dynamoDbResults.keys()) - set(queryResults.keys())
    common_keys = set(queryResults.keys()).intersection(dynamoDbResults.keys())
    different_values = {key: (queryResults[key], dynamoDbResults[key]) for key in common_keys if queryResults[key] != dynamoDbResults[key]}
    print(f'total results in database {len(queryResults)}')
    print(f'total results in dynamo {len(dynamoDbResults)}')
    print(f'number of matching records in db and dynamo {len(common_keys)}')
    print(f'number of missing records in dynamo {len(dynamoMissing)}')
    print(f'number of missing records in db {len(dbMissing)}')
    print(f'missing records in db {dbMissing}')
    print(f'total number of record with difference in spending are  {len(different_values)}')
    for key,values in different_values.items():
        print(f'for the campTime {key} the value is different in db and dynamo as {values}')



print(runner("2023-11-11 00:00:00","2023-11-11 23:00:00",18789))
print(runner("2023-11-11 00:00:00","2023-11-11 23:00:00",53890))
print(runner("2023-11-11 00:00:00","2023-11-11 23:00:00",47049))
print(runner("2023-11-11 00:00:00","2023-11-11 23:00:00",43192))


# def getDynamo():
#     data  = dynamo(40200,"2023-10-31 00:00:00","2023-11-31 24:00:00",)
#     highest_value = max(data.values())
#     print(str(datetime.datetime.fromtimestamp(highest_value)))
#     current_time = int(datetime.datetime.now().timestamp())
#     print(current_time)
#     timeDiff = current_time - highest_value
#     print(timeDiff/60)

def dynamoGetTimeDelats(diff1= 0, diff2 =0) -> int:
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('rtb-beeswax-spend-records-prod')
    attribute_name = "ExpirationTime"
    ttl = timedelta(days=7)
    today = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start = int((today + ttl).timestamp())
    end = int((today + ttl + timedelta(days=1)).timestamp())
    all_items =[]
    response = None
    i = 0
    total_Scans = 10

    # while response is None or 'LastEvaluatedKey' in response or i <= 10:
    while i <= 1:
        print(f'running loop {i}')
        if response is None:
            response = table.scan(
                FilterExpression="ExpirationTime BETWEEN :start_date AND :end_date",
                ExpressionAttributeValues = {
                ":start_date": start,
                ":end_date": end
            }
            )
        else:
            response = table.scan(
                FilterExpression="ExpirationTime BETWEEN :start_date AND :end_date",
                ExpressionAttributeValues={
                    ":start_date": start,
                    ":end_date": end
                },
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
        all_items.extend(response['Items'])
        i += 1
    counter = 0
    deltas = []
    for data in all_items:
        counter +=1
        createtime = datetime.fromtimestamp(int(data.get("CreationTime")))
        bidtime = datetime.fromtimestamp(int(data.get("Timestamp") / 1000000))
        diff = (int(data.get("CreationTime") - int(data.get("Timestamp") / 1000000)))/60
        print(f'createtime {createtime} and bidtime {bidtime} and diff {diff}')
        deltas.append((data.get("CreationTime") - data.get("Timestamp")/1000000))
    print(counter)
    return (sum(deltas) / len(deltas))/60

# print(dynamoGetTimeDelats(diff1=0,diff2=10))