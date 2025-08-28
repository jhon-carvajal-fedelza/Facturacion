import json
import boto3
import random
import string
import uuid
import hashlib
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import parse_qs

REGION = 'us-east-1'
URL_SQS_SENDEMAIL = 'https://sqs.us-east-1.amazonaws.com/381491847136/'
LOGIN_REDIRECT_URL = "https://portal-fe.fedelza.com/" # URL a donde redirigir al usuario

# Inicializar el cliente de DynamoDB
dynamodb = boto3.resource('dynamodb')
# Configurar el cliente de SQS
sqs = boto3.client('sqs', region_name=REGION)

def send_sqs(url_sqs: str, message: str) -> None:
    """
    Esta función realiza el envio a las colas de SQS.
    """
    try:
        response = sqs.send_message(
            QueueUrl=url_sqs,
            MessageBody=message
        )
        print(response)
    except Exception as e:
        print(e)
        raise

def get_activation_info(table_name: str, token: str):
    """
    Busca el token de activación en la tabla y devuelve la información del usuario.
    """
    print("Buscando token en la BD")
    table = dynamodb.Table(table_name)
    response = table.get_item(Key={'activationToken': token})
    return response.get('Item')

def update_user_status(table_name: str, customer_tin: str, email: str):
    """
    Actualiza el estado de la cuenta del usuario a activo.
    """
    table = dynamodb.Table(table_name)
    
    # La clave de tu tabla es 'customerTin' y 'email'
    key = {'customerTin': customer_tin, 'email': email}
    
    response = table.update_item(
        Key=key,
        UpdateExpression="SET activeUser = :val",
        ExpressionAttributeValues={
            ':val': True
        },
        ReturnValues="UPDATED_NEW"
    )
    print("Usuario actualizado:", response['Attributes'])
    return response

def get_user_status(table_name: str, customer_tin: str, email: str):
    """
    Consulta el estado de la cuenta del usuario.
    """
    table = dynamodb.Table(table_name)
    
    # La clave de tu tabla es 'customerTin' y 'email'
    key = {'customerTin': customer_tin, 'email': email}

    #Campo a consultar
    projection_expression = 'activeUser'
    
    response = table.get_item(
        Key=key,
        ProjectionExpression=projection_expression
    )

    return response

def send_confirmation_email(environment: str, recipient_email: str, name: str, password: str):
    """
    Envía un correo de confirmación de activación.
    """
    message_body = json.dumps({
        "sender": "confirmacion@fedelza.com",
        "recipient": recipient_email,
        "template": "Fedelza_ConfirmacionActivacion",
        "data": {
            "nombre": name,
            "contrasena": password,
            "url": LOGIN_REDIRECT_URL
        }
    })
    
    url_queue = f"{URL_SQS_SENDEMAIL}{environment}_Email_Send-ondemand"
    send_sqs(url_queue, message_body)

def get_user_data(table_name: str, customer_tin: str, email: str):
    """
    Obtiene los datos del usuario para el correo de confirmación.
    """
    table = dynamodb.Table(table_name)
    response = table.get_item(Key={'customerTin': customer_tin, 'email': email})
    return response.get('Item')

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

def generar_contraseña_segura():
    longitud = 12
    # Caracteres obligatorios
    mayuscula = random.choice(string.ascii_uppercase)
    minuscula = random.choice(string.ascii_lowercase)
    numero = random.choice(string.digits)
    simbolo = random.choice("!@#$%^&*()-_=+[]{}|;:,.<>?")

    # Resto de caracteres aleatorios
    restantes = longitud - 4
    todos_los_caracteres = string.ascii_letters + string.digits + "!@#$%^&*()-_=+[]{}|;:,.<>?"
    otros = random.choices(todos_los_caracteres, k=restantes)

    # Combinar y mezclar todos los caracteres
    contraseña = list(mayuscula + minuscula + numero + simbolo + ''.join(otros))
    random.shuffle(contraseña)

    return ''.join(contraseña)

def created_hash(password: str):
    """
    Genera un salt y un hash SHA-256 para la contraseña.
    """
    salt = str(uuid.uuid4())
    salted_password = password + salt
    hash_object = hashlib.sha256(salted_password.encode('utf-8'))
    hashed_password = hash_object.hexdigest()
    return salt, hashed_password


def lambda_handler(event, context):
    """
    Función Lambda para validar un token de activación de cuenta.
    """
    try:
        # Obtener el token de activación de los parámetros de la URL
        query_string = event['queryStringParameters']
        activation_token = query_string.get('token')
        print(f"token: {activation_token}")
        if not activation_token:
            return {
                'statusCode': 400,
                'message': 'Token de activación no proporcionado.'
            }
        
        # Determinar el entorno de la función para el nombre de la tabla
        nombre_funcion = context.function_name
        environment = context.invoked_function_arn.split(":")[-1]
        environment = "Dev" if environment == nombre_funcion else environment
        
        activation_table_name = f"{environment}_userActivation"
        user_table_name = f"{environment}_user"
        
        # 1. Validar el token en la tabla de activaciones
        print(f"Nombre tabla: {activation_table_name}")
        activation_data = get_activation_info(activation_table_name, activation_token)
        
        if not activation_data:
            return {
                'statusCode': 404,
                'message': 'Token inválido o ya utilizado.'
            }
        
        # 2. Verificar si el token ha expirado
        now_timestamp = int(datetime.utcnow().timestamp())
        expiration_timestamp = activation_data['expirationTimestamp']
        
        if now_timestamp > expiration_timestamp:
            return {
                'statusCode': 410,
                'message': 'El token de activación ha expirado.'
            }
            
        customer_tin = activation_data['customerTin']
        email = activation_data['email']
        print(f"customerTin: {customer_tin}")
        
        # 3. Obtener los datos del usuario para la activación
        # (Necesitamos el email para la clave de la tabla de usuarios)
        user_data = get_user_data(user_table_name, customer_tin, activation_data.get('email'))
        if not user_data:
            return {
                'statusCode': 404,
                'message': 'Usuario no encontrado.'
            }

        print("Informacion del usuario consultada")

        # Verificar si el usuario ya está activo
        user_status = get_user_status(user_table_name, customer_tin, user_data['email'])
        if user_status.get('Item', {}).get('activeUser'):
            # Si el usuario ya esta activo es porque la peticion viene del proceso de recuperacion de contraseña
            #Debo realizar la creacion de la nueva contraseña, actualizarla y enviarla
            # Actualizar la contraseña del usuario en DynamoDB

            new_password = generar_contraseña_segura()
            print(f"new_password: {new_password}")

            # Generar el nuevo hash y salt
            new_salt, new_hashed_password = created_hash(new_password)
            print(f"new_salt: {new_salt}")
            print(f"new_hashed_password: {new_hashed_password}")

            if not update_user_password(user_table_name, str(customer_tin), email, new_salt, new_hashed_password):
                return {
                    'statusCode': 500,
                    'message': 'Error al actualizar la contraseña.'
                }

            # Preparar el mensaje para la cola SQS
            user_name = user_data.get('firstName', 'Usuario')
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
                'statusCode': 302,
                'headers': {
                    'Location': LOGIN_REDIRECT_URL,
                    'Content-Type': 'text/html'
                },
                'body': '<html><body><h1>Tu cuenta ha sido activada con éxito.</h1><p>Redirigiendo al inicio de sesión...</p></body></html>'
            }
            
        
        # 4. Actualizar el estado del usuario
        update_user_status(user_table_name, customer_tin, user_data['email'])
        print("Usuario actualizado a activo")
        # 5. Enviar correo de confirmación
        name = user_data.get('firstName', '')
        password = user_data.get('initialPassword', '')
        #name = user_data.get('firstName', user_data.get('companyName', ''))

        send_confirmation_email(environment, user_data['email'], name, password)
        
        # 6. Eliminar el token de activación
        table = dynamodb.Table(activation_table_name)
        table.delete_item(Key={'activationToken': activation_token})

        # 7. Redirigir al usuario
        return {
            'statusCode': 302,
            'headers': {
                'Location': LOGIN_REDIRECT_URL,
                'Content-Type': 'text/html'
            },
            'body': '<html><body><h1>Tu cuenta ha sido activada con éxito.</h1><p>Redirigiendo al inicio de sesión...</p></body></html>'
        }

    except ClientError as e:
        print(f"Error de DynamoDB: {e}")
        return {
            'statusCode': 500,
            'message': 'Error al procesar la solicitud de activación.'
        }
    except Exception as e:
        print(f"Error inesperado: {e}")
        return {
            'statusCode': 500,
            'message': 'Error interno del servidor.'
        }