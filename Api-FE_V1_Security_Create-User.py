import os
import re
import uuid
import boto3
import hashlib
from zoneinfo import ZoneInfo
from datetime import datetime

dynamodb = boto3.resource("dynamodb")
EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$')

def lambda_handler(event, context):
    try:
        environment = context.invoked_function_arn.split(":")[-1]
        table_name = f"{environment}_user"
        table = dynamodb.Table(table_name)

        # Obtener la fecha y hora actual
        now = datetime.now(ZoneInfo("America/Bogota"))
        # Formatear la fecha y hora según un formato específico
        formatted_date = now.strftime("%Y-%m-%d %H:%M:%S")


        #leer los datos de entrada
        customer_tin = str(event.get('identificationNumber', '')).strip()
        email = event.get('email', '').strip().lower()
        password = event.get('password', '').strip()
        name = event.get('name', '').strip()
        phone = event.get('phone', '').strip()
        role = event.get('role', '').strip()
        enabled = event.get('enabled', True)

        # Validaciones básicas
        if not customer_tin or not email or not password or not name:
            return {"statusCode": 400, "message": "Faltan campos obligatorios."}

        if not EMAIL_REGEX.match(email):
            return {"statusCode": 400, "message": "El email no tiene un formato válido."}

        # Verificar si el usuario ya existe
        response = table.get_item(Key={"customerTin": customer_tin,"email": email})
        if "Item" in response:
            return {"statusCode": 409, "message": "El usuario ya existe para este NIT."}

        # Generar un salt aleatorio
        salt = str(uuid.uuid4())
        # Concatenar la contraseña y el salt
        saltedPassword = password + salt
        # Crear un objeto hash (SHA-256 en este caso)
        hash_object = hashlib.sha256(saltedPassword.encode())
        hashed_password = hash_object.hexdigest()

        item = {
            "customerTin": customer_tin,
            "email": email,
            "name": name,
            "phone": phone,
            'userHash': hashed_password,
            'userSalt': salt,
            "role": role,
            "activeUser": enabled,
            "date": formatted_date
        }

        table.put_item(Item=item)

        return {
            "statusCode": 201,
            "message": "Usuario creado exitosamente.",
            "email": email,
            "tin": customer_tin
        }

    except Exception as e:
        print("Error:", str(e))
        return {
            "statusCode": 500,
            "message": "Error interno al crear el usuario.",
            "error": str(e)
        }