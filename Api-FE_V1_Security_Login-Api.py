import re
import os
import boto3
import hashlib
import jwt
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

dynamodb = boto3.resource("dynamodb")

EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$')
SECRET_KEY = os.environ.get('secretJWT', '')
JWT_ALGORITHM = "HS256"
TOKEN_EXPIRATION_MINUTES = 60

def lambda_handler(event, context):
    try:
        # Preparar tabla según entorno
        environment = context.invoked_function_arn.split(":")[-1]
        table_name = f"{environment}_user"
        table = dynamodb.Table(table_name)

        # Leer parámetros de entrada
        customer_tin = str(event.get("identificationNumber", "")).strip()
        email = event.get("email", "").strip().lower()
        password = event.get("password", "").strip()

        if not customer_tin or not email or not password:
            return {"statusCode": 400, "message": "Faltan datos obligatorios."}

        if not EMAIL_REGEX.match(email):
            return {"statusCode": 400, "message": "El email no tiene un formato válido."}


        # Obtener el usuario desde DynamoDB
        response = table.get_item(Key={"customerTin": customer_tin, "email": email})
        user = response.get("Item")

        if not user:
            return {"statusCode": 404, "message": "Usuario no encontrado."}

        # Validar contraseña
        stored_salt = user.get("userSalt")
        stored_hash = user.get("userHash")
        hashed_input = hashlib.sha256((password + stored_salt).encode()).hexdigest()

        if hashed_input != stored_hash:
            return {"statusCode": 401, "message": "Credenciales inválidas."}

        # Generar JWT
        now = datetime.now(ZoneInfo("America/Bogota"))
        exp = now + timedelta(minutes=TOKEN_EXPIRATION_MINUTES)

        payload = {
            "customerTin": customer_tin,
            "email": email,
            "name": user.get("name"),
            "role": user.get("role"),
            "exp": int(exp.timestamp()),
            "iat": int(now.timestamp())
        }

        token = jwt.encode(payload, SECRET_KEY, algorithm=JWT_ALGORITHM)

        return {
            "statusCode": 200,
            "message": "Inicio de sesión exitoso.",
            "token": token,
            "user": {
                "identificationNumber": customer_tin,
                "email": email,
                "name": user.get("name"),
                "role": user.get("role")
            }
        }

    except Exception as e:
        print("Error:", e)
        return {"statusCode": 500, "message": "Error interno.", "error": str(e)}
