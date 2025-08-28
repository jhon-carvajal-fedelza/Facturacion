import boto3
import os

dynamodb = boto3.resource("dynamodb")

def lambda_handler(event, context):
    try:
        # Obtener nombre de la tabla basado en el entorno
        environment = context.invoked_function_arn.split(":")[-1]
        table_name = f"{environment}_user"
        table = dynamodb.Table(table_name)

        # Obtener datos del evento
        tin = event.get("identificationNumber")
        email = event.get("email")

        if not tin or not email:
            return {
                "statusCode": 400,
                "message": "Se requieren los campos 'identificationNumber' y 'email' para eliminar el usuario."
            }

        # Intentar eliminar el usuario
        response = table.delete_item(
            Key={
                "customerTin": tin,
                "email": email
            },
            ConditionExpression="attribute_exists(email)"
        )

        return {
            "statusCode": 200,
            "message": f"Usuario con email '{email}' eliminado correctamente."
        }

    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return {
            "statusCode": 404,
            "message": "El usuario no existe o ya fue eliminado."
        }
    except Exception as e:
        print("Error al eliminar usuario:", e)
        return {
            "statusCode": 500,
            "message": "Error interno al eliminar el usuario.",
            "error": str(e)
        }
