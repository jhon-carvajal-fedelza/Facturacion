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
    description = "Plantilla eliminada correctamente"
    statusCode = 201

    try:
        # Obtener datos del evento
        userId = event['userId'] 
        templateName = event['templateName']

    except:
        status = False
        statusCode = 500
        description = "Error no controlado en el servicio"
    else:
        # Eliminar la plantilla de correo electrónico
        try:
            response = ses_client.delete_template(
                TemplateName=templateName
            )
            print("plantilla eliminada correctamente")
            
            
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
            'description':description
        }

    return response