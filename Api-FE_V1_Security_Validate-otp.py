import os
import json
import boto3
import jwt
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import OneTimePassword as otp

JWT_ALGORITHM = "HS256"
TOKEN_EXPIRATION_MINUTES = 60
SECRET_KEY = os.environ.get('secretJWT', '')

dynamodb = boto3.resource("dynamodb")

def lambda_handler(event, context):
    try:
        environment = context.invoked_function_arn.split(":")[-1]
        table_name = f"{environment}_user"
        table = dynamodb.Table(table_name)
        customer_tin = str(event.get("identificationNumber", "")).strip()
        otp_number = event['otp']
        email = event['email']
        ip = event['ip']
        print(f"Otp: {otp}")
        print(f"email: {email}")
        print(f"Ip: {ip}")

        # Realizar la validación del OTP
        validation_result = otp.validateOTP(environment, otp_number, email, ip)

        if validation_result['status']:
            # Generar JWT
            now = datetime.now(ZoneInfo("America/Bogota"))
            exp = now + timedelta(minutes=TOKEN_EXPIRATION_MINUTES)

            # Obtener el usuario desde DynamoDB
            response = table.get_item(Key={"customerTin": customer_tin, "email": email})
            user = response.get("Item")

            name = user.get("name","")

            payload = {
                "customerTin": customer_tin,
                "email": email,
                "name": name,
                "role": user.get("role"),
                "exp": int(exp.timestamp()),
                "iat": int(now.timestamp())
            }
            token = jwt.encode(payload, SECRET_KEY, algorithm=JWT_ALGORITHM)
            return {
                'statusCode': 200,
                "message": "Inicio de sesión exitoso.",
                "token": token,
                    "user": {
                    "name": name,
                    "role": user.get("role")
                }
            }
        else:
            return {
                'statusCode': 400,
                'message': validation_result['description']
            }

    except KeyError as e:
        return {
            'statusCode': 400,
            'message':f'Falta el parámetro en el cuerpo de la solicitud: {e}'
        }
    except Exception as e:
        print(f"Error en la función Lambda: {e}")
        return {
            'statusCode': 500,
            'message': 'Error interno del servidor'
        }