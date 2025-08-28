import boto3
import hashlib
import uuid

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
        new_password = event.get("newPassword")

        if not tin or not email or not new_password:
            return {
                "statusCode": 400,
                "message": "Se requieren los campos 'identificationNumber', 'email' y 'newPassword'."
            }

        # Generar nuevo salt y hash
        new_salt = str(uuid.uuid4())
        salted_password = new_password + new_salt
        hashed_password = hashlib.sha256(salted_password.encode()).hexdigest()

        # Actualizar en DynamoDB
        table.update_item(
            Key={
                "customerTin": str(tin),
                "email": email
            },
            UpdateExpression="SET userHash = :hash, userSalt = :salt",
            ExpressionAttributeValues={
                ":hash": hashed_password,
                ":salt": new_salt
            },
            ConditionExpression="attribute_exists(email)"
        )

        return {
            "statusCode": 200,
            "message": "Contraseña actualizada correctamente."
        }

    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return {
            "statusCode": 404,
            "message": "El usuario no existe."
        }
    except Exception as e:
        print("Error:", e)
        return {
            "statusCode": 500,
            "message": "Error al actualizar la contraseña.",
            "error": str(e)
        }
