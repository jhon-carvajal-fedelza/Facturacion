import boto3
import os

dynamodb = boto3.resource("dynamodb")

def lambda_handler(event, context):
    try:
        # Obtener entorno y tabla
        environment = context.invoked_function_arn.split(":")[-1]

        #Pruebas
        environment = "Dev"

        table_name = f"{environment}_customerDianConfig"
        table = dynamodb.Table(table_name)

        # Extraer el customerId del evento
        customer_id = event.get("customerId")
        if not customer_id:
            return {
                "statusCode": 400,
                "message": "Falta el campo 'customerId'."
            }

        # Consultar todas las resoluciones del cliente
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key("customerId").eq(customer_id)
        )

        return {
            "statusCode": 200,
            "resolutions": response.get("Items", [])
        }

    except Exception as e:
        print("Error al obtener resoluciones:", e)
        return {
            "statusCode": 500,
            "message": "Error interno al consultar resoluciones.",
            "error": str(e)
        }
