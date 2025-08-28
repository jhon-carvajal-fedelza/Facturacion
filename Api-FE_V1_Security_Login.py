import re
import os
import json
import boto3
import hashlib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import OneTimePassword as otp

REGION = 'us-east-1'
dynamodb = boto3.resource("dynamodb")
# Configurar el cliente de SQS
sqs = boto3.client('sqs', region_name=REGION)

EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$')

URL_SQS_SENDEMAIL = 'https://sqs.us-east-1.amazonaws.com/381491847136/'

def send_sqs(url_sqs:str,message:list)->None:
    """
    Esta función realiza el envio a las colas de SQS.

    Args:
        url_sqs (str): Url de la cola SQS en AWS
        messages (list): Lista con los mensajes (Maximo 10)

    Returns:
        dict: Nombre de la campaña
    """

    try:
        response = sqs.send_message(
            QueueUrl=url_sqs,
            MessageBody=message
        )
        print(response)

    except Exception as e:
        print(e)

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
        ip = "0.0.0.0"

        if not customer_tin or not email or not password:
            return {"statusCode": 400, "message": "Faltan datos obligatorios."}

        if not EMAIL_REGEX.match(email):
            return {"statusCode": 400, "message": "El email no tiene un formato válido."}


        # Obtener el usuario desde DynamoDB
        response = table.get_item(Key={"customerTin": customer_tin, "email": email})
        user = response.get("Item")
        name = user.get("name","")
        if not user:
            return {"statusCode": 404, "message": "Usuario no encontrado."}

        # Validar contraseña
        stored_salt = user.get("userSalt")
        stored_hash = user.get("userHash")
        hashed_input = hashlib.sha256((password + stored_salt).encode()).hexdigest()

        if hashed_input != stored_hash:
            return {"statusCode": 401, "message": "Credenciales inválidas."}

    
        expiration_minutes = 10
        template_email = "Fedelza_EnvioOTP"
        otp_number = otp.createOTP(environment,"Auth",email,ip,expiration_minutes)
        print(f"OTP: {otp_number}")

        message = json.dumps({
            "sender":"confirmacion@fedelza.com",
            "recipient":email,
            "template":template_email,
            "data":{
                "nombre":name,
                "otp_code":str(otp_number),
                "expiration":expiration_minutes
            }
        })
        # Enviar mensaje a la cola de envios        
        send_sqs(f"{URL_SQS_SENDEMAIL}{environment}_Email_Send-ondemand",message)

        return {
            "statusCode": 200,
            "message": "Inicio de sesión exitoso."            
        }

    except Exception as e:
        print("Error:", e)
        return {"statusCode": 500, "message": "Error interno.", "error": str(e)}
