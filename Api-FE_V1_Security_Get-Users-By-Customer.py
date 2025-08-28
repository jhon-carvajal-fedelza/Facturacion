import boto3
import os

dynamodb = boto3.resource("dynamodb")
REGION = 'us-east-1'

def lambda_handler(event, context):
    nit = event.get("nit")
    environment = context.invoked_function_arn.split(":")[-1]
    table_name = f"{environment}_userApi"
    table = dynamodb.Table(table_name)

    try:
        response = table.scan(
            FilterExpression="customerId = :cid",
            ExpressionAttributeValues={":cid": nit},
            ProjectionExpression="nombres, apellidos, correo, telefono, rol, activeUser"
        )
        items = response.get("Items", [])

        usuarios = []
        for item in items:
            usuarios.append({
                "nombres": item.get("nombres", ""),
                "apellidos": item.get("apellidos", ""),
                "correo": item.get("correo", ""),
                "telefono": item.get("telefono", ""),
                "rol": item.get("rol", ""),
                "estado": item.get("activeUser", True)
            })

        return usuarios

    except Exception as e:
        print("Error:", e)
        return {
            "error": str(e)
        }
