import boto3
from boto3.dynamodb.conditions import Key
import os

dynamodb = boto3.resource("dynamodb")

def lambda_handler(event, context):
    try:
        # Obtener entorno desde ARN de Lambda
        environment = context.invoked_function_arn.split(":")[-1]
        table_name = f"{environment}_user"
        table = dynamodb.Table(table_name)

        # Validación de claves principales
        tin = event.get("identificationNumber")
        email = event.get("email")
        if not tin or not email:
            return {
                "statusCode": 400,
                "message": "Los campos 'tin' y 'email' son obligatorios para identificar el usuario."
            }

        # Obtener el usuario desde DynamoDB
        response = table.get_item(Key={"customerTin": tin, "email": email})
        user = response.get("Item")

        if not user:
            return {"statusCode": 404, "message": "Usuario no encontrado."}

        # Filtrar solo campos válidos para actualizar
        fields_to_update = {k: v for k, v in event.items() if k not in ("identificationNumber", "email") and v not in (None, "", [], {})}

        if not fields_to_update:
            return {
                "statusCode": 400,
                "message": "No se proporcionaron campos para actualizar."
            }

        # Construir expresión de actualización
        update_expression = "SET " + ", ".join(f"#{k} = :{k}" for k in fields_to_update.keys())
        expression_attribute_names = {f"#{k}": k for k in fields_to_update.keys()}
        expression_attribute_values = {f":{k}": v for k, v in fields_to_update.items()}

        # Ejecutar actualización
        table.update_item(
            Key={"customerTin": tin, "email": email},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values
        )

        return {
            "statusCode": 200,
            "message": "Usuario actualizado correctamente."
        }

    except Exception as e:
        print("Error al actualizar el usuario:", e)
        return {
            "statusCode": 500,
            "message": "Error interno al actualizar el usuario.",
            "error": str(e)
        }
