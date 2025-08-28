import boto3
import uuid
from botocore.exceptions import ClientError
from datetime import datetime

REGION = 'us-east-1'

# Inicializar el cliente SES
ses_client = boto3.client('ses', region_name=REGION)
# Configurar el cliente de DynamoDB
dynamodb = boto3.resource('dynamodb')

table_channel = dynamodb.Table('channel')
table_customer = dynamodb.Table('customer')
tabla_consecutive = dynamodb.Table('templateControl')
table_templateAudit = dynamodb.Table('templateAudit')

def consult_consecutive(customerId):
    #Inicio de consulta del consecutivo para la campaña
    #consulta por scan
    consecutive = "0000"       
    projectionConsecutive_expression = 'numeration'  # Lista de campos a consultar

    responseConsecutive = tabla_consecutive.scan(
        FilterExpression="customerId = :value",
        ExpressionAttributeValues={":value": customerId},
        ProjectionExpression=projectionConsecutive_expression
    )
    if responseConsecutive['Items']:
        consecutive = responseConsecutive['Items'][0]['numeration']

    consecutiveInt = int(consecutive)
    consecutiveInt += 1
    consecutiveString = str(consecutiveInt)
    consecutive = consecutiveString.zfill(4)
    return consecutive

def update_consecutive(customerId,consecutive):
    if (consecutive == "0001"):
        # Debo realizar el insert a la tabla 
        templateControlId = str(uuid.uuid4())
        # Insertar datos en la tabla de consecutivos
        tabla_consecutive.put_item(
            Item={
                'templateControlId': templateControlId,
                'customerId': customerId,
                'numeration': consecutive
            }
        )
    else:           
        projectionConsecutive_expression = 'templateControlId'  # Lista de campos a consultar
        responseId = tabla_consecutive.scan(
            FilterExpression='customerId = :c',
            ExpressionAttributeValues={':c': customerId},
            ProjectionExpression=projectionConsecutive_expression
        )
        if responseId['Items']:
            templateControlId = responseId['Items'][0]['templateControlId']

        responseUpdateConsecutive = tabla_consecutive.update_item(
            Key={'templateControlId':templateControlId},
            UpdateExpression='SET numeration = :s',
            ExpressionAttributeValues={':s': consecutive},
            ReturnValues='UPDATED_NEW'
        )
        print(responseUpdateConsecutive['Attributes'])

def select_customerName(customerId):
    projectionCustomer_expression = 'company'  # Lista de campos a consultar

    response = table_customer.scan(
        FilterExpression="customerId = :value",
        ExpressionAttributeValues={":value": customerId},
        ProjectionExpression=projectionCustomer_expression
    )
    return response['Items'][0]['company']

def select_channelName(channelId):
    projectionName_expression = 'channelName'  # Lista de campos a consultar

    response = table_channel.scan(
        FilterExpression="channelId = :value",
        ExpressionAttributeValues={":value": int(channelId)},
        ProjectionExpression=projectionName_expression
    )
    return response['Items'][0]['channelName']

  

def insert_audit(userId,templateName,action):
    templateAuditId = str(uuid.uuid4())
    # Obtener la fecha y hora actual
    now = datetime.utcnow()
    # Formatear la fecha y hora según un formato específico
    formattedDate = now.strftime("%Y-%m-%d %H:%M:%S")
    
    # Insertar datos en la tabla de datos de usuarios
    table_templateAudit.put_item(
        Item={
            'templateAuditId': templateAuditId,
            'userId': userId,
            'templateName': templateName,
            'action': action,
            'date': formattedDate
        }
    )

def create_template(template_name, subject, html_body, text_body):
    """Crea una plantilla de SES."""
    ses_client.create_template(
        Template={
            'TemplateName': template_name,
            'SubjectPart': subject,
            'HtmlPart': html_body,
            'TextPart': text_body
        }
    )
    mensaje = f"Plantilla '{template_name}' creada correctamente."
    print(mensaje)
    return mensaje

def update_template(template_name, subject, html_body, text_body):
    """Actualiza una plantilla de SES."""
    ses_client.update_template(
        Template={
            'TemplateName': template_name,
            'SubjectPart': subject,
            'HtmlPart': html_body,
            'TextPart': text_body
        }
    )
    mensaje = f"Plantilla '{template_name}' actualizada correctamente."
    print(mensaje)
    return mensaje

def create_template_ant(templateName,subject,htmlBody,textBody):    
    print("template:" + templateName)
    response = ses_client.create_template(
        Template={
            'TemplateName': templateName,
            'SubjectPart': subject,
            'HtmlPart': htmlBody,
            "TextPart": textBody
        }
    )
    print("plantilla creada correctamente")  

def create_or_update_template(template_name, subject, html_body, text_body):
    """Verifica si la plantilla existe y la crea o actualiza."""
    try:
        print("Intentando")
        # Intenta obtener la plantilla para ver si existe
        ses_client.get_template(TemplateName=template_name)
        print("Plantilla encontrada, actualizando...")
        # Si no hay excepción, la plantilla existe, entonces la actualizamos
        return update_template(template_name, subject, html_body, text_body)
        
    except ClientError as e:
        code = e.response['Error']['Code']
        print("Error al obtener la plantilla:", code)
        if e.response['Error']['Code'] == 'TemplateDoesNotExist':
            # Si la plantilla no existe, la creamos
            print("Plantilla no encontrada, creando...")
            return create_template(template_name, subject, html_body, text_body)
            
        else:
            # Si es otro tipo de error, lo relanzamos
            print("Error inesperado:", e)
            raise  

    except Exception as e:
        print("Excepción no controlada:")
        traceback.print_exc()  # <-- Esto mostrará el error en CloudWatch
        raise


def lambda_handler(event, context):
    status = True
    description = "Plantilla creada correctamente"
    statusCode = 201

    try:
        # Obtener datos del evento
        customerName = event['customerName']
        templateName = event['templateName']
        subject = event['subject']
        htmlBody = event['htmlBody']
        textBody = event['textBody']
    except Exception as e:
        print(e)
        status = False
        statusCode = 500
        description = "Error no controlado en el servicio"
    else:
        # Crear la plantilla de correo electrónico
        try:            
            
            #Realizar la creacion del template en AWS SES
            try:  
                # Crear o actualizar la plantilla en SES
                print("Creando plantilla en SES")
                response = create_or_update_template(templateName, subject, htmlBody, textBody)   
                print("Plantilla creada correctamente")  
                description = response             
                #response = create_template(templateName,subject,htmlBody,textBody)
            except:
                status = False
                statusCode = 500
                description = "Error realizando la creacion del template en SES"

            
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