import json
import os
import subprocess
from datetime import date
from typing import List, Dict
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

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



def dynamo(cmp_id:str,date:str) -> List[Dict[str, any]]:
    retArr = []
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('BeeswaxCampaignSpendByDeviceTypeAggregates-prod')
    newJson = {}
    for i in range(0, 24):
        # zero-pad the hour to ensure it's always two digits
        hour_str = str(i).zfill(2)
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('CampaignId_TimeBucket').eq(
                f'{cmp_id}#{date}{hour_str}')
        )
        items = response['Items']
        for data in items:
            newJson[f'{data["CampaignId_TimeBucket"]}_{data["DeviceType"]}'] =  int(data["TotalSpendMicros"])
    return newJson

def run_query_and_collect_results(start_date:str,end_date:str,camp_id:int) -> list:
    database_url = "postgresql://{0}:{1}@{2}:{3}/{4}".format(user, pwd, host, port, db)
    engine = create_engine(database_url)
    query = text(f"""
        SELECT c.platform_device_type,
               HOUR(c.imp_rx_time_utc) AS bid_hour,
               c.line_item_id AS camp_id,
               (SUM(c.win_cost_micros_usd)) AS hourly_spend
        FROM win_logs c
        WHERE c.time >= '{start_date}'
              AND c.time < '{end_date}'
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
    print(f'test results for the {cmpId} for the dates {startDate} to {endDate}')
    queryResults = run_query_and_collect_results(startDate,endDate,cmpId)
    sd = startDate.split(" ")[0].replace("-","")
    dynamoDbResults = dynamo(cmpId,sd)
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
print(runner("2023-09-13 00:00:00","2023-09-14 23:00:00",32503))
