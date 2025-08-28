'''
Lambda para realizar la recepcion de todos los estados de emails enviados
'''
import json
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3

global customer_name
global process_id
global message_id
global timestamp
global state

#pylint: disable=C0301
REGION = 'us-east-1'
#Configurar el cliente de DynamoDB
dynamodb = boto3.resource('dynamodb', region_name=REGION)

#states
#1	Enviado
#2	Entregado
#3	Rechazado
#4	Abierto
#5	Clicado
#6	Rebote
#7	Queja
#8	FallaRenderizado
#9	Retrazado
#10	Suscrito

#Mapeo de estados de SES
state_ses_mapping = { 
    'Send': 1,
    'Delivery': 2,
    'Reject': 3,
    'Open': 4,
    'Click': 5,
    'Bounce': 6,
    'Complaint': 7,
    'Rendering Failure': 8,
    'DeliveryDelay': 9,
    'Subscription': 10
}

########################

#NO DEBO ACEPTAR NOMBRES DE CLIENTES DE MAS DE 50 CARACTERES

########################

#bounce:
#- timestamp
#- bounceType - bounceSubType
#  Undetermined - Undetermined: Amazon SES no ha podido determinar un motivo específico de rebote.
#  Permanent - General: Amazon SES recibió un rechazo permanente general. Si recibe este tipo de rebote, debería eliminar la dirección de correo electrónico del destinatario de su lista de correo.
#  Permanent - NoEmail: Amazon SES recibió un rechazo permanente porque la dirección de correo electrónico de destino no existe. Si recibe este tipo de rebote, debería eliminar la dirección de correo electrónico del destinatario de su lista de correo.
#  Permanent - Suppressed: Amazon SES ha suprimido el envío a esta dirección dado que tiene un historial reciente de rebotes como dirección no válida. Para anular la lista de supresión global, consulte Uso de la lista de supresión de nivel de cuenta de Amazon SES.
#  Permanent - OnAccountSuppressionList: Amazon SES ha suprimido el envío a esta dirección porque está en la lista de supresión de nivel de cuenta. Esto no se toma en cuenta para calcular la métrica de porcentaje de rebotes.
#  Transient - General: Amazon SES recibió un rebote general. Es posible que pueda enviar correctamente a este destinatario en el futuro.
#  Transient - MailboxFull: Amazon SES ha recibido un rebote completo de bandeja de entrada. Es posible que pueda enviar correctamente a este destinatario en el futuro.
#  Transient - MessageTooLarge: Amazon SES recibió un rebote de mensaje demasiado grande. Es posible que pueda enviar correctamente a este destinatario si reduce el tamaño del mensaje.
#  Transient - ContentRejected: Amazon SES ha recibido un rebote de contenido rechazado. Es posible que pueda enviar correctamente a este destinatario si cambia el contenido del mensaje.
#  Transient - AttachmentRejected: Amazon SES ha recibido un rebote de archivo adjunto rechazado. Es posible que pueda enviar correctamente a este destinatario si elimina o cambia el archivo adjunto.

#complaint
#- timestamp
#- complaintFeedbackType
#  abuse: Indica correo electrónico no solicitado o algún otro tipo de abuso de correo electrónico.
#  auth-failure: Informe de error de autenticación de correo electrónico.
#  fraud: Indica algún tipo de fraude o actividad de phishing.
#  not-span: Indica que la entidad que proporciona el informe no considera el mensaje como spam. Esto se puede utilizar para corregir un mensaje que estaba mal etiquetado o clasificado como spam.
#  other: Indica cualquier otra retroalimentación que no encaje en otros tipos registrados.
#  virus: Notifica que se ha encontrado un virus en el mensaje de origen.

#deliveryDelay
#- timestamp
#- delayType
#  InternalFailure: un problema interno de Amazon SES provocó que el mensaje se retrasara.
#  General: se produjo un error genérico durante la conversación SMTP.
#  MailboxFull: el buzón del destinatario está lleno y no puede recibir mensajes adicionales.
#  SpamDetected: el servidor de correo del destinatario detectó una gran cantidad de correos electrónicos no solicitados de su cuenta.
#  RecipientServerError: un problema temporal con el servidor de correo electrónico del destinatario impide la entrega del mensaje.
#  IPFailure: el proveedor de correo electrónico del destinatario bloquea o limita la dirección IP que envía el mensaje.
#  TransientCommunicationFailure: hubo un error temporal de comunicación durante la conversación SMTP con el proveedor de correo electrónico del destinatario.
#  BYOIPHostNameLookupUnavailable: Amazon SES no pudo buscar el nombre de anfitrión DNS para sus direcciones IP. Este tipo de retraso únicamente se produce cuando se utiliza Bring Your Own IP.
#  Undetermined: Amazon SES no pudo determinar el motivo del retraso en la entrega.
#  SendingDeferral: Amazon SES ha considerado apropiado aplazar de forma interna el mensaje.

#delivery
#- timestamp

#suscription
#- timestamp

#click
#- timestamp
#- ipAddress
#- link
#- linkTags

#open
#- timestamp
#- ipAddress

#reject
#- reason: La razón por la que se rechazó el correo electrónico. El único valor posible es Bad content, lo que significa que Amazon SES detectó que el correo electrónico contenía un virus. Cuando se rechaza un mensaje, Amazon SES detiene el procesamiento y no intenta entregarlo al servidor de correo del destinatario.

#failure (Error en el renderizado de plantillas SES)
#- errorMessage
def format_date(date):
    formatted_date = date.strftime("%Y-%m-%dT%H:%M:%S%z")
    formatted_date = formatted_date[:-2] + ":" + formatted_date[-2:]
    return formatted_date

def insert_blacklist(table_blacklist,email:str,num_fac,rejection_type:str,description:str)->None:
    """
    Esta función realiza el insert de los registros a la tabla de lista negra.

    Args:
        customer_name (str): Nombre del cliente
        date (str): Fecha de insercion
        email (str): Email del cliente
        rejection_type (str): Tipo de rechazo
        description (str): Descripcion del rechazo

    Returns:
        None
    """
    # Obtener la fecha y hora actual
    formatted_date = format_date(datetime.now(ZoneInfo("America/Bogota")))
    
    # Insertar datos en la tabla de lista negra
    table_blacklist.put_item(
        Item={
            'email': email,
            'date': formatted_date,
            'numFac': num_fac,
            'rejectionType': rejection_type,
            'description': description
        }
    )

def insert_status(table_send_status,num_fac,message_id,date,customer_name,customer_id,email,state,type1:str,type2:str)->None:
    """
    Esta función obtiene los datos de la campaña.

    Args:
        campaignName (str): Nombre de la campana

    Returns:
        dict: Nombre de la campaña
    """
    

    # Insertar datos en la tabla de lista negra
    table_send_status.put_item(
        Item={
            'numFac': num_fac,
            'messageId': message_id,
            'date': date,
            'customerName': customer_name,
            'customerId': customer_id,
            'email': email,
            'state': state,
            'type1': type1,
            'type2': type2
        }
    )

def update_traceability_process(table_name:str,num_fac:str,step:int)->None:
    table_traceability_process = dynamodb.Table(table_name)
    try:
        response = table_traceability_process.update_item(
            Key={
                'numFac': num_fac
            },
            UpdateExpression="SET step = :step",
            ExpressionAttributeValues={
                ':step': step
            },
            ReturnValues="UPDATED_NEW"
        )
    except Exception as e:
        print(e)

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
    #Obtener el mensaje SNS
    body = event["Records"][0]["body"]
    json_body = json.loads(body)
    print(json_body)
    #message_sqs = event['Records'][0]["body"]['Message']
    message = json.loads(json_body['Message'])

    #Extraer el estado de SES y el messageId
    event_type = message['eventType']
    message_mail = message['mail']
    message_id = message_mail['messageId']
    print("MessageId: " + message_id)
    print("Event: " + event_type)

    email = message_mail['destination'][0]

    #Captura de tags
    tags = message_mail['tags']
    supplier_id = tags['supplierId'][0]    
    customer_name = tags['customerName'][0]
    customer_id = int(tags['customerId'][0])
    num_fac = tags['numFac'][0]

    bucket_name = f"fe.{environment}.{supplier_id}"
    table_send_status = dynamodb.Table(f'{environment}_{supplier_id}_sendStatus')
    table_blacklist = dynamodb.Table(f'{environment}_{supplier_id}_blacklist')
    traceability_process_table_name = f"{environment}_{supplier_id}_traceabilityProcess"

    #TODO
    #Revisar si es necesario ingresar trazabilidad en este punto
    #traceability_process_table_name = f"{supplier_id}_traceabilityProcess"
    #update_traceability_process(traceability_process_table_name,num_fac,5)

    # Mapear el estado de SES a un nombre legible
    state = state_ses_mapping.get(event_type,0) #Estado desconocido
    timestamp = message_mail['timestamp']
    print("Supplier: " + supplier_id)
    ###################
    #Mas probables
    #Send    
    if state == 1:
        insert_status(table_send_status,num_fac,message_id,timestamp,customer_name,customer_id,email,state,"","")
    #Delivery
    elif state == 2:
        timestamp = message['delivery']['timestamp']
        insert_status(table_send_status,num_fac,message_id,timestamp,customer_name,customer_id,email,state,"","")
        update_traceability_process(traceability_process_table_name,num_fac,5)
    #Open
    elif state == 4:
        timestamp = message['open']['timestamp']
        ip_address = message['open']['ipAddress']
        insert_status(table_send_status,num_fac,message_id,timestamp,customer_name,customer_id,email,state,ip_address,"")
    #Click
    elif state == 5:
        timestamp = message['click']['timestamp']
        ip_address = message['click']['ipAddress']
        link = message['click']['link']
        insert_status(table_send_status,num_fac,message_id,timestamp,customer_name,customer_id,email,state,ip_address,link)

    ###################
    #Probables
    #Reject
    elif state == 3:
        reason = message['reject']['reason']
        insert_status(table_send_status,num_fac,message_id,timestamp,customer_name,customer_id,email,state,reason,"")
        update_traceability_process(traceability_process_table_name,num_fac,5)
    #Bounce
    elif state == 6:
        timestamp = message['bounce']['timestamp']
        bounce_type = message['bounce']['bounceType']
        bounce_subtype = message['bounce']['bounceSubType']
        if bounce_type == "Permanent":
            #Enviar a la lista negra
            email = message['bounce']['bouncedRecipients'][0]['emailAddress']
            insert_blacklist(table_blacklist,email,num_fac,bounce_type,bounce_subtype)
            update_traceability_process(traceability_process_table_name,num_fac,5)
        #Insertar estado
        insert_status(table_send_status,num_fac,message_id,timestamp,customer_name,customer_id,email,state,bounce_type,bounce_subtype)

    ###################
    #Menos probables
    #Complaint
    elif state == 7:
        timestamp = message['complaint']['timestamp']
        complaint_feedback_type = message['complaint']['complaintFeedbackType']
        insert_status(table_send_status,num_fac,message_id,timestamp,customer_name,customer_id,email,state,complaint_feedback_type,"")
    #Rendering Failure
    elif state == 8:
        error_message = message['failure']['errorMessage']
        insert_status(table_send_status,num_fac,message_id,timestamp,customer_name,customer_id,email,state,error_message,"")
    #DeliveryDelay
    elif state == 9:
        timestamp = message['deliveryDelay']['timestamp']
        delay_type = message['deliveryDelay']['delayType']
        insert_status(table_send_status,num_fac,message_id,timestamp,customer_name,customer_id,email,state,delay_type,"")
    #Subscription
    elif state == 10:
        timestamp = message['subscription']['timestamp']
        insert_status(table_send_status,num_fac,message_id,timestamp,customer_name,customer_id,email,state,"","")
    else:
        pass
