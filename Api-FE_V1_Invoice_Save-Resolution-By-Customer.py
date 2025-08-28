import boto3
import os
from zoneinfo import ZoneInfo
from datetime import datetime

dynamodb = boto3.resource("dynamodb")

def lambda_handler(event, context):
    environment = context.invoked_function_arn.split(":")[-1]
    table_name = f"{environment}_customerDianConfig"
    table = dynamodb.Table(table_name)

    # Obtener la fecha y hora actual
    now = datetime.now(ZoneInfo("America/Bogota"))
    # Formatear la fecha y hora según un formato específico
    formatted_date = now.strftime("%Y-%m-%d %H:%M:%S")

    try:
        item = {
            "customerId": event["identificationNumber"],
            "enabled": {"BOOL": True},
            "initialRange": {"N": str(event["range"]["initialRange"])},
            "finalRange": {"N": str(event["range"]["finalRange"])},
            "numberingExpiration": {"S": event["numberingExpiration"]},
            "prefix": {"S": event["prefix"]},
            "authorizationNumber": {"S": event["authorizationNumber"]},
            "createdAt": {"S": formatted_date}
        }

        table.put_item(Item=item)

        return {
                "statusCode": 200,
                "message": "resolución guardada."
            }

    except Exception as e:
        print("Error:", e)
        return {
                "statusCode": 400,
                "message": "Error"
            }
