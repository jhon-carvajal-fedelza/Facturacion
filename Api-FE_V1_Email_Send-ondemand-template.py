'''
Lambda para realizar los envios de notificaciones internas de fedelza
'''
import re
import uuid
import json
from datetime import datetime, timedelta

import boto3

REGION = 'us-east-1'

# Crea un cliente SES
ses = boto3.client('ses',region_name=REGION)

# Configurar el cliente de DynamoDB
dynamodb = boto3.resource('dynamodb')

def insert_send_ondemand(table_name:str,email_id:str,sender:str,data:str,email:str,template:str,state:str,description:str)->None:
    """
    Esta función realiza el insert del registro a la tabla de detalles del proceso.

    Args:
        sender (str): Cuenta de email desde donde se realiza el envio
        email (str): Email del cliente
        template (str): Nombre del template configurado en SES
        template_version (int): Version del template que se desea enviar
        state (str): Estado del envio
        error_description (str): Descripcion detallada del error
        
    Returns:
        None: No retorna resultados
    """

    # Obtener la fecha y hora actual
    now = datetime.utcnow()
    colombia_time = now - timedelta(hours=5)
    # Formatear la fecha y hora según un formato específico
    formatted_date = colombia_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'
    table_email_ondemand = dynamodb.Table(table_name)
    # Insertar datos en la tabla de campañas
    table_email_ondemand.put_item(
        Item={
            'email_id': email_id,
            'sender': sender,
            'recipient': email,
            'data': data,
            'template': template,
            'dateSend': formatted_date,
            'state': state,
            'description': description
        }
    )

def extract_variables(template_str):
    return set(re.findall(r'{{\s*([^}]+?)\s*}}', template_str))

def validate_template_data(template_html, data_json):
    variables = extract_variables(template_html)
    data_dict = json.loads(data_json)
    missing = variables - set(data_dict.keys())
    return missing

def lambda_handler(event, context):
    """
    Función principal

    Args:
        event (dict): Datos de evento
        context (dict): Datos de contexto
        
    Returns:
        None: Personalizado
    """
    environment = context.invoked_function_arn.split(":")[-1]

    status = True
    state = "Enviado"
    description = "Envio realizado correctamente"
    status_code = 200
    print(event)
    email_id = ""

    configuration_set = f"{environment}-configuration-set-Fedelza"
    table_name = f"{environment}_email_ondemand"

    # Obtener datos del evento
    # Validar si se disparo elevento desde SQS o es desde api gateway
    if 'Records' in event:
        event = json.loads(event['Records'][0]['body'])

    sender = event['sender']
    recipient = event['recipient']
    template = event['template']
    data = json.dumps(event['data']) #Se espera que llegue un diccionario con los nombres acorde a lo que esta en el template


    print("data: " + data)


    # Obtiene la información de la plantilla
    response_template = ses.get_template(TemplateName=template)
    html = response_template["Template"]["HtmlPart"]    

    missing_vars = validate_template_data(html, data)

    if missing_vars:
        state = "Error"
        description = f"Faltan las siguientes variables en TemplateData: {missing_vars}"
        raise ValueError(description)
    else:
        patron_email = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z0-9]{2,}$'
        if not re.match(patron_email, recipient):
            status = False
            state = "Error"
            status_code = 400
            description = f"El email {recipient} no tiene una estructura correcta"
            print(description)
        else:
            tags = [{
                "Name":"customer",
                "Value":"mailconnect"
            },
            {
                "Name":"campaingId",
                "Value":"generic"
            },
            {
                "Name":"processId",
                "Value":"default"
            }]
            try:
                # Envía el correo electrónico
                response = ses.send_templated_email(
                    ConfigurationSetName=configuration_set,
                    Source=sender,
                    Destination={'ToAddresses': [recipient]},
                    Template=template,
                    TemplateData=data,
                    Tags=tags
                )
                email_id = response.get('MessageId', str(uuid.uuid4())+"-Error")
                print("Correo electrónico enviado correctamente")
                print(f"email id: {email_id}")
            except ses.exceptions.ClientError as e:
                # Maneja diferentes tipos de errores
                state = "Error"
                if e.response['Error']['Code'] == 'TemplateDoesNotExist':
                    error_description = f"Error: La plantilla '{template}' no existe."
                elif e.response['Error']['Code'] == 'SenderNotFound':
                    error_description = f"Error: El remitente '{sender}' no se encuentra."
                else:
                    error_description = f"Error: {e.response['Error']['Message']}"
                print(e.response['Error'])
                print(error_description)
                description = error_description

    insert_send_ondemand(table_name,email_id,sender,data,recipient,template,state,description)
    # Respuesta
    response = {
        'status':status,
        'statusCode': status_code,
        'description':description,
    }

    return response
