import re
import json
import uuid
import string
import random
import hashlib
import boto3
#from OneTimePassword import createOTP
import OneTimePassword as otp
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

REGION = 'us-east-1'
URL_SQS_SENDEMAIL = 'https://sqs.us-east-1.amazonaws.com/381491847136/'

# Inicializar el cliente de DynamoDB
dynamodb = boto3.resource('dynamodb')
# Configurar el cliente de SQS
sqs = boto3.client('sqs', region_name=REGION)
# Cliente s3
s3 = boto3.client("s3")

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

def exist_companyTin(table_name:str,company_tin):
    # Verificar si el usuario ya existe
    table_user_api = dynamodb.Table(table_name)
    response = table_user_api.get_item(Key={'userApiId': company_tin})

    if 'Item' in response:
        return True
    else:
        return False

def exist_user(table_name:str,customer_tin,email):
    # Verificar si el usuario ya existe
    table_user = dynamodb.Table(table_name)
    response = table_user.get_item(Key={'customerTin':customer_tin,'email': email})

    if 'Item' in response:
        return True
    else:
        return False

def check_and_create_table(tableName, pk):

    key_schema = [
        {'AttributeName': pk, 'KeyType': 'HASH'}
    ]
    attribute_definitions = [
        {
            'AttributeName': pk,
            'AttributeType': 'S'
        }
    ]
    try:
        # Intenta crear la tabla
        table = dynamodb.create_table(
            TableName=tableName,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            BillingMode='PAY_PER_REQUEST'  # Configura capacidad bajo demanda
        )
        print(f"La tabla '{tableName}' ha sido creada con éxito.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"La tabla '{tableName}' ya existe.")
        else:
            print("Error al crear la tabla:", e)

def create_s3_bucket(bucket_name):
    try:
        # Crear el bucket
        s3.create_bucket(
            Bucket=bucket_name
        )
        
        # Habilitar el versionado del bucket
        s3.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={'Status': 'Enabled'}
        )
        
        '''
        # Hacer que todos los objetos sean privados por defecto
        bucket_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "DenyPublicRead",
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket_name}/*"
                }
            ]
        }
        
        s3.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(bucket_policy))
        '''

        # Configurar ciclo de vida: 1 año en Standard, luego a Glacier, luego eliminar en 10 años
        lifecycle_configuration = {
            "Rules": [
                {
                    "ID": "MoveToGlacier",
                    "Filter": {},
                    "Status": "Enabled",
                    "Transitions": [
                        {
                            "Days": 365,  # Después de 1 año, mover a Glacier
                            "StorageClass": "GLACIER"
                        }
                    ],
                    "Expiration": {
                        "Days": 3650  # Eliminar después de 10 años
                    },
                    "NoncurrentVersionTransitions": [
                        {
                            "NoncurrentDays": 90,  # Versiones antiguas a Glacier en 90 días
                            "StorageClass": "GLACIER"
                        }
                    ],
                    "NoncurrentVersionExpiration": {
                        "NoncurrentDays": 3650  # Eliminar versiones antiguas en 10 años
                    }
                }
            ]
        }

        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_configuration
        )

        print(f"Bucket {bucket_name} creado y configurado correctamente.")
    
    except Exception as e:
       print(f"Error: {str(e)}")

def check_and_create_table_dian_audit(tableName, pk, sk):

    key_schema = [
        {'AttributeName': pk, 'KeyType': 'HASH'},  # Partition Key
        {'AttributeName': sk, 'KeyType': 'RANGE'}  # Sort Key
    ]

    attribute_definitions = [
        {'AttributeName': pk, 'AttributeType': 'S'},
        {'AttributeName': sk, 'AttributeType': 'S'},
        {'AttributeName': 'customerName', 'AttributeType': 'S'},
        {'AttributeName': 'customerId', 'AttributeType': 'N'},
        {'AttributeName': 'invoiceDate', 'AttributeType': 'S'},
        {'AttributeName': 'dianStatus', 'AttributeType': 'S'}
    ]

    global_secondary_indexes = [
        {
            'IndexName': 'GSI_customerName',
            'KeySchema': [{'AttributeName': 'customerName', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'}
        },
        {
            'IndexName': 'GSI_customerId',
            'KeySchema': [{'AttributeName': 'customerId', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'}
        },
        {
            'IndexName': 'GSI_invoiceDate',
            'KeySchema': [{'AttributeName': 'invoiceDate', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'}
        },
        {
            'IndexName': 'GSI_dianStatus',
            'KeySchema': [{'AttributeName': 'dianStatus', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'}
        }
    ]

    try:
        # Crear la tabla con GSIs
        table = dynamodb.create_table(
            TableName=tableName,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            BillingMode='PAY_PER_REQUEST',  # Capacidad bajo demanda
            GlobalSecondaryIndexes=global_secondary_indexes
        )
        print(f"La tabla '{tableName}' ha sido creada con éxito.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"La tabla '{tableName}' ya existe.")
        else:
            print("Error al crear la tabla:", e)
            
def check_and_create_table_dian_status(tableName, pk):

    key_schema = [
        {'AttributeName': pk, 'KeyType': 'HASH'}  # Partition Key
    ]

    attribute_definitions = [
        {'AttributeName': pk, 'AttributeType': 'S'},
        {'AttributeName': 'customerName', 'AttributeType': 'S'},
        {'AttributeName': 'customerId', 'AttributeType': 'N'},
        {'AttributeName': 'cufe', 'AttributeType': 'S'},
        {'AttributeName': 'invoiceDate', 'AttributeType': 'S'},
        {'AttributeName': 'dianStatus', 'AttributeType': 'S'},
        {'AttributeName': 'prefix', 'AttributeType': 'S'}
    ]

    global_secondary_indexes = [
        {
            'IndexName': 'GSI_customerName',
            'KeySchema': [{'AttributeName': 'customerName', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'}
        },
        {
            'IndexName': 'GSI_customerId',
            'KeySchema': [{'AttributeName': 'customerId', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'}
        },
        {
            'IndexName': 'GSI_cufe',
            'KeySchema': [{'AttributeName': 'cufe', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'}
        },
        {
            'IndexName': 'GSI_invoiceDate',
            'KeySchema': [{'AttributeName': 'invoiceDate', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'}
        },
        {
            'IndexName': 'GSI_dianStatus',
            'KeySchema': [{'AttributeName': 'dianStatus', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'}
        },
        {
            'IndexName': 'GSI_prefix',
            'KeySchema': [{'AttributeName': 'prefix', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'}
        }
    ]

    try:
        # Crear la tabla con GSIs
        table = dynamodb.create_table(
            TableName=tableName,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            BillingMode='PAY_PER_REQUEST',  # Capacidad bajo demanda
            GlobalSecondaryIndexes=global_secondary_indexes
        )
        print(f"La tabla '{tableName}' ha sido creada con éxito.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"La tabla '{tableName}' ya existe.")
        else:
            print("Error al crear la tabla:", e)

def check_and_create_table_send(tableName, pk):

    key_schema = [
        {'AttributeName': pk, 'KeyType': 'HASH'}  # Partition Key
    ]

    attribute_definitions = [
        {'AttributeName': pk, 'AttributeType': 'S'},
        {'AttributeName': 'customerName', 'AttributeType': 'S'},
        {'AttributeName': 'customerId', 'AttributeType': 'N'},
        {'AttributeName': 'email', 'AttributeType': 'S'},
        {'AttributeName': 'stateSend', 'AttributeType': 'S'}
    ]

    global_secondary_indexes = [
        {
            'IndexName': 'GSI_customerName',
            'KeySchema': [{'AttributeName': 'customerName', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'}
        },
        {
            'IndexName': 'GSI_customerId',
            'KeySchema': [{'AttributeName': 'customerId', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'}
        },
        {
            'IndexName': 'GSI_email',
            'KeySchema': [{'AttributeName': 'email', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'}
        },
        {
            'IndexName': 'GSI_stateSend',
            'KeySchema': [{'AttributeName': 'stateSend', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'}
        }
    ]

    try:
        # Crear la tabla con GSIs
        table = dynamodb.create_table(
            TableName=tableName,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            BillingMode='PAY_PER_REQUEST',  # Capacidad bajo demanda
            GlobalSecondaryIndexes=global_secondary_indexes
        )
        print(f"La tabla '{tableName}' ha sido creada con éxito.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"La tabla '{tableName}' ya existe.")
        else:
            print("Error al crear la tabla:", e)

def check_and_create_table_blacklist(tableName, pk, sk):

    key_schema = [
        {'AttributeName': pk, 'KeyType': 'HASH'},  # Partition Key
        {'AttributeName': sk, 'KeyType': 'RANGE'}  # Sort Key
    ]

    attribute_definitions = [
        {'AttributeName': pk, 'AttributeType': 'S'},
        {'AttributeName': sk, 'AttributeType': 'S'},
        {'AttributeName': 'numFac', 'AttributeType': 'S'}
    ]

    global_secondary_indexes = [
        {
            'IndexName': 'GSI_numFac',
            'KeySchema': [{'AttributeName': 'numFac', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'}
        }
    ]

    try:
        # Crear la tabla con GSIs
        table = dynamodb.create_table(
            TableName=tableName,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            BillingMode='PAY_PER_REQUEST',  # Capacidad bajo demanda
            GlobalSecondaryIndexes=global_secondary_indexes
        )
        print(f"La tabla '{tableName}' ha sido creada con éxito.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"La tabla '{tableName}' ya existe.")
        else:
            print("Error al crear la tabla:", e)

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

def created_hash(password:str):
    # Generar un salt aleatorio
    salt = str(uuid.uuid4())        
    # Concatenar la contraseña y el salt
    saltedPassword = password + salt
    # Crear un objeto hash (SHA-256 en este caso)
    hash_object = hashlib.sha256(saltedPassword.encode())
    hashed_password = hash_object.hexdigest()
    return salt, hashed_password

def lambda_handler(event, context):
    try:
        activation_token = str(uuid.uuid4()) 
        nombre_funcion = context.function_name
        environment = context.invoked_function_arn.split(":")[-1]
        environment = "Dev" if environment == nombre_funcion else environment
        # Validar la entrada
        #body = json.loads(event.get('body', '{}'))
    
        # Ejemplo de uso
        api_password = generar_contraseña_segura()
        platform_password = generar_contraseña_segura()

        print(f"Api password: {api_password}")
        print(f"Platform password: {platform_password}")

        #Datos de registro de cuenta
        account_type = event['accountType']
        document_type = event['documentType']
        identification_number = str(event['identificationNumber'])
        verification_digit = event['verificationDigit']
        company_name = event['companyName']
        first_name = event['firstName']
        first_last_name = event['firstLastName']
        middle_last_name = event['middleLastName']
        address = event['address']
        country = event['country']
        state = event['state']
        city = event['city']
        postal_code = event['postalCode']
        economic_activity = event['economicActivity']
        phone = event['phone']
        account_email = event['accountEmail']
        billing_email = event['billingEmail']
        accept_policies = event['acceptsPolicies']
        ip = event['ip']
        expiration_minutes = 10
        expiration_hours = 24
        # Obtener la fecha y hora actual
        now = datetime.now(ZoneInfo("America/Bogota"))
        # Sumar las horas de expiracion a la fecha
        expiration_date = now + timedelta(hours=expiration_hours)
        expiration_timestamp = int(expiration_date.timestamp())
        formated_expiration_date = expiration_date.strftime("%Y-%m-%d %H:%M:%S%z")
        formated_expiration_date = formated_expiration_date[:-2] + ":" + formated_expiration_date[-2:]
        # Formatear la fecha y hora según un formato específico
        formatted_date = now.strftime("%Y-%m-%d %H:%M:%S%z")
        formatted_date = formatted_date[:-2] + ":" + formatted_date[-2:]
        #ormatted_date = colombia_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + 'Z'

        print(f"Identificacion: {identification_number}")
        
        if not identification_number:
            return {
                'statusCode': 400,
                'message': 'El NIT es obligatorios.'
            }
        
        
        user_api_table_name = f"{environment}_userApi"
        user_table_name = f"{environment}_user"
        print(f"Nombre de la tabla: {user_api_table_name}")
        print(f"Nombre de la tabla: {user_table_name}")
        # Verificar si el usuario del API ya existe
        if (exist_companyTin(user_api_table_name,identification_number)):
            return {
                'statusCode': 409,
                'message': 'El usuario del API ya existe.'
            }
        print("Usuario del API no existe")

        # Verificar si el usuario de la plataforma ya existe
        if (exist_user(user_table_name,identification_number,account_email)):
            return {
                'statusCode': 409,
                'message': 'El usuario de la plataforma ya existe.'
            }
        print("Usuario de la plataforma no existe")

        # Guardar el usuario del API en la tabla
        salt, hashed_password = created_hash(api_password)
        print(f"API Salt: {salt}")
        print(f"API Hashed Password: {hashed_password}")
        table_user_api = dynamodb.Table(user_api_table_name)
        table_user_api.put_item(
            Item={
                'userApiId':identification_number,
                'accountType':account_type,
                'documentType':document_type,
                'identificationNumber':identification_number,
                'verificationDigit':verification_digit,
                'companyName':company_name,
                'firstName':first_name,
                'firstLastName':first_last_name,
                'middleLastName':middle_last_name,
                'address':address,
                'country':country,
                'state':state,
                'city':city,
                'postalCode':postal_code,
                'economicActivity':economic_activity,
                'phone':phone,
                'accountEmail':account_email,
                'billingEmail':billing_email,
                'acceptsPolicies':accept_policies,
                'ip':ip,
                'initialPassword': api_password,
                'userHash': hashed_password,
                'userSalt': salt,
                'activeUser':False,
                'date':formatted_date
            }
        )
        name = ""
        if account_type == "Persona Jurídica":
            name = company_name
        elif account_type == "Persona Natural":
            name = first_name

        # Guardar el usuario de la plataforma en la tabla
        salt, hashed_password = created_hash(platform_password)
        print(f"Platform Salt: {salt}")
        print(f"Platform Hashed Password: {hashed_password}")        
        table_user = dynamodb.Table(user_table_name)
        table_user.put_item(
            Item={
                'customerTin':identification_number,
                'email':account_email,                
                'documentType':document_type,
                'identificationNumber':identification_number,                                
                'firstName':name,
                'firstLastName':first_last_name,
                'middleLastName':middle_last_name,                
                'phone':phone,
                'initialPassword':platform_password,               
                'userHash': hashed_password,
                'userSalt': salt,
                'activeUser':False,
                'role': "user",
                'date':formatted_date
            }
        )
        
        #Insertar registro en tabla de activacion
        activation_table_name = f"{environment}_userActivation"
        table_user_activation = dynamodb.Table(activation_table_name)
        table_user_activation.put_item(
            Item={
                'activationToken':activation_token,
                'customerTin':identification_number,
                'email': account_email,
                'expirationDate':formated_expiration_date,
                'expirationTimestamp':expiration_timestamp,
                'date':formatted_date
            }
        )

        #crear bucket fe.{company} con regla de ciclo de vida
        bucket_name = f"fe.{environment.lower()}.{identification_number}" 
        create_s3_bucket(bucket_name)
        #Crear tabla {company}_traceabilityProcess
        traceability_process_table_name = f"{environment}_{identification_number}_traceabilityProcess"
        dian_audit_table_name = f"{environment}_{identification_number}_dianAudit"
        dian_status_table_name = f"{environment}_{identification_number}_dianStatus"
        send_detail_table_name = f"{environment}_{identification_number}_sendDetail"
        send_status_table_name = f"{environment}_{identification_number}_sendStatus"
        black_list_table_name = f"{environment}_{identification_number}_blacklist"
        check_and_create_table(traceability_process_table_name,"numFac")
        #Crear tabla {environment}_{company}_dianAudit
        check_and_create_table_dian_audit(dian_audit_table_name,"numFac","date")
        #Crear tabla {environment}_{company}_dianStatus
        check_and_create_table_dian_status(dian_status_table_name,"numFac")
        #Crear tabla {environment}_{company}_sendDetail
        check_and_create_table_send(send_detail_table_name,"numFac")
        #Crear tabla {environment}_{company}_sendStatus
        check_and_create_table_send(send_status_table_name,"numFac")
        #Crear tabla {environment}_{company}_blackList
        check_and_create_table_blacklist(black_list_table_name,"email","date")        
        
        #otp_number = otp.createOTP(environment,"Auth",account_email,ip,expiration_minutes)
        #print(f"OTP: {otp_number}")

        url = f"https://fe.fedelza.com/{environment}/V1/Security/Acount-activation?token={activation_token}"
        print(f"Url activación: {url}")
        
        message = json.dumps({
            "sender":"confirmacion@fedelza.com",
            "recipient":account_email,
            "template":"Fedelza_ActivacionCuenta",
            "data":{
                "nombre":name,
                "url":url,
                "expiration":expiration_hours
            }
        })
        
        # Enviar mensaje a la cola de envios        
        send_sqs(f"{URL_SQS_SENDEMAIL}{environment}_Email_Send-ondemand",message)
        

        return {
            'statusCode': 201,
            'message': 'Usuario registrado exitosamente.'
        }
    
    except ClientError as e:
        return {
            'statusCode': 500,
            'message': f'Error al acceder a DynamoDB. error: {str(e)}'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'message': f'Error interno del servidor. error: {str(e)}'
        }
