import json
import uuid
import hashlib
import boto3
import string
import random
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import unquote

# Inicializar el cliente de DynamoDB y SQS
dynamodb = boto3.resource('dynamodb')
sqs = boto3.client('sqs')

REGION = 'us-east-1'
URL_SQS_SENDEMAIL = 'https://sqs.us-east-1.amazonaws.com/381491847136/'
LOGIN_REDIRECT_URL = "https://portal-fe.fedelza.com/" # URL a donde redirigir al usuario

def send_sqs(url_sqs: str, message: str) -> None:
    """
    Envía un mensaje a la cola SQS.
    """
    try:
        sqs.send_message(
            QueueUrl=url_sqs,
            MessageBody=message
        )
        print("Mensaje enviado a SQS con éxito.")
    except Exception as e:
        print(f"Error al enviar mensaje a SQS: {e}")

def get_user_by_email(table_name: str, identification_number:str, email: str):
    """
    Busca un usuario en la tabla de DynamoDB por su email.
    """
    try:
        table = dynamodb.Table(table_name)
        response = table.get_item(Key={'customerTin':identification_number, 'email': email})
        return response.get('Item')
    except ClientError as e:
        print(f"Error al obtener usuario de DynamoDB: {e}")
        return None

def update_user_password(table_name: str, identification_number:str, email: str, new_salt: str, new_hash: str):
    """
    Actualiza el hash y salt de la contraseña del usuario en DynamoDB.
    """
    try:
        table = dynamodb.Table(table_name)
        table.update_item(
            Key={'customerTin':identification_number, 'email': email},
            UpdateExpression="SET userSalt = :salt, userHash = :hash",
            ExpressionAttributeValues={
                ':salt': new_salt,
                ':hash': new_hash
            }
        )
        return True
    except ClientError as e:
        print(f"Error al actualizar la contraseña en DynamoDB: {e}")
        return False


def lambda_handler(event, context):
    try:
        # Obtener el nombre de la tabla de usuarios desde el ARN de la función
        nombre_funcion = context.function_name
        environment = context.invoked_function_arn.split(":")[-1]
        environment = "Dev" if environment == nombre_funcion else environment
        user_table_name = f"{environment}_user"

        identification_number = event.get('identificationNumber')
        email = event.get('email')
        print(f"identificationNumber: {identification_number}")
        print(f"email: {email}")

        if not identification_number:
            print("El número de identificación es obligatorio.")
            return {
                'statusCode': 400,
                'message': 'El número de identificación es obligatorio.'
            }

        if not email:
            print("El email es obligatorio.")
            return {
                'statusCode': 400,
                'message': 'El email es obligatorio.'
            }

        # Buscar el usuario por su email
        user = get_user_by_email(user_table_name, str(identification_number), email)
        if not user:
            print("Usuario no encontrado.")
            return {
                'statusCode': 200,
                'message': 'Nueva contraseña generada y enviada al correo.'
            }

        name = user.get('name','')

        activation_token = str(uuid.uuid4())
        # Generar una nueva contraseña segura
        

        expiration_hours = 24

        now = datetime.now(ZoneInfo("America/Bogota"))
        # Sumar las horas de expiracion a la fecha
        expiration_date = now + timedelta(hours=expiration_hours)
        expiration_timestamp = int(expiration_date.timestamp())
        formated_expiration_date = expiration_date.strftime("%Y-%m-%dT%H:%M:%S%z")
        formated_expiration_date = formated_expiration_date[:-2] + ":" + formated_expiration_date[-2:]
        # Formatear la fecha y hora según un formato específico
        formatted_date = now.strftime("%Y-%m-%dT%H:%M:%S%z")
        formatted_date = formatted_date[:-2] + ":" + formatted_date[-2:]
        
        

        #Insertar registro en tabla de activacion
        activation_table_name = f"{environment}_userActivation"
        table_user_activation = dynamodb.Table(activation_table_name)
        table_user_activation.put_item(
            Item={
                'activationToken':activation_token,
                'customerTin':identification_number,
                'email': email,
                'expirationDate':formated_expiration_date,
                'expirationTimestamp':expiration_timestamp,
                'date':formatted_date
            }
        )

        url = f"https://fe.fedelza.com/{environment}/V1/Security/Acount-activation?token={activation_token}"
        print(f"Url activación: {url}")
        
        message = json.dumps({
            "sender":"confirmacion@fedelza.com",
            "recipient":email,
            "template":"Fedelza_ActivacionCuenta",
            "data":{
                "nombre":name,
                "url":url,
                "expiration":expiration_hours
            }
        })
        
        # Enviar mensaje a la cola de envios        
        send_sqs(f"{URL_SQS_SENDEMAIL}{environment}_Email_Send-ondemand",message)
        
        
        ######ESTO SE HACE DESDE OTRO SERVICIO######}
        '''
        # Actualizar la contraseña del usuario en DynamoDB
        if not update_user_password(user_table_name, str(identification_number), email, new_salt, new_hashed_password):
            return {
                'statusCode': 500,
                'message': 'Error al actualizar la contraseña.'
            }

        # Preparar el mensaje para la cola SQS
        user_name = user.get('firstName', 'Usuario')
        message = json.dumps({
            "sender": "no-reply@fedelza.com",
            "recipient": email,
            "template": "Fedelza_RecuperacionContrasena",
            "data": {
                "nombre": user_name,
                "contrasena": new_password,
                "url": LOGIN_REDIRECT_URL
            }
        })


        # Enviar el mensaje a la cola SQS para notificar al usuario
        sqs_queue_url = f"{URL_SQS_SENDEMAIL}{environment}_Email_Send-ondemand"
        send_sqs(sqs_queue_url, message)

        return {
            'statusCode': 200,
            'message': 'Nueva contraseña generada y enviada al correo.'
        }
        '''


        return {
            'statusCode': 200,
            'message': 'Url de reestablecimiento enviada al correo.'
        }
    
    except json.JSONDecodeError:
        return {
            'statusCode': 400,
            'message': 'Formato de JSON inválido.'
        }
    except ClientError as e:
        return {
            'statusCode': 500,
            'message': f'Error de AWS: {str(e)}'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'message': f'Error interno del servidor: {str(e)}'
        }