import boto3
from boto3.dynamodb.conditions import Key
import os

dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    try:
        environment = context.invoked_function_arn.split(":")[-1]
        table_name = f"{environment}_user"
        table = dynamodb.Table(table_name)

        customer_tin = str(event.get("identificationNumber", "")).strip()
        if not customer_tin:
            return {
                "statusCode": 400,
                "message": "El campo 'identificationNumber' es obligatorio."
            }

        # Consultar todos los usuarios por NIT
        response = table.query(
            KeyConditionExpression=Key("customerTin").eq(customer_tin)
        )

        users = response.get("Items", [])
        if not users:
            return {
                "statusCode": 200,
                "message": f"No se encontraron usuarios para el NIT {customer_tin}.",
                "data": []
            }

        formatted_users = []
        for user in users:
            formatted_users.append({
                "email": user.get("email"),
                "name": user.get("name"),
                "phone": user.get("phone"),
                "role": user.get("role"),
                "enabled": user.get("activeUser"),
                "date": user.get("date")
            })

        return {
            "statusCode": 200,
            "message": f"{len(formatted_users)} usuario(s) encontrados.",
            "data": formatted_users
        }

    except Exception as e:
        print("Error:", e)
        return {
            "statusCode": 500,
            "message": "Error interno al consultar los usuarios.",
            "error": str(e)
        }
