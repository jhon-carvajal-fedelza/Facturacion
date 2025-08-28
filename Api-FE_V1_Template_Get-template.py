import boto3
import uuid
from datetime import datetime

# Inicializar el cliente SES
ses_client = boto3.client('ses', region_name='us-east-1')
# Configurar el cliente de DynamoDB
dynamodb = boto3.resource('dynamodb')

table_templateAudit = dynamodb.Table('templateAudit')

def lambda_handler(event, context):
    status = True
    description = "Plantilla recuperada correctamente"
    statusCode = 200
    responseTemplate = ""

    try:
        # Obtener datos del evento
        userId = event['userId'] 
        templateName = event['templateName']
    except:
        status = False
        statusCode = 500
        description = "Error no controlado en el servicio"
    else:
        # Crear la plantilla de correo electrónico
        try:
            responseTemplate = ses_client.get_template(TemplateName=templateName)
            print(responseTemplate)
            print("plantilla recuperada correctamente")
            
            
        except Exception as e:
            print(e)
            status = False
            statusCode = 500
            description = "Error no controlado en el servicio"
    finally:
        # Respuesta
        response = {
            'status':status,
            'statusCode': statusCode,
            'description':description,
            'template':responseTemplate['Template']
        }

    return response