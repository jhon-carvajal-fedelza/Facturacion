import json
from datetime import datetime

import boto3
import io
import time

REGION = 'us-east-1'
URL_SQS_SENDEMAIL = 'https://sqs.us-east-1.amazonaws.com/381491847136/'

# Configurar el cliente de SQS
sqs = boto3.client('sqs', region_name=REGION)

# Inicializar el cliente de DynamoDB
dynamodb = boto3.resource('dynamodb', region_name=REGION)

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

def update_traceability_process(table_name:str,num_fac:str,step:int,pdf_creation_start_date:str,pdf_creation_duration:str)->None:
    table_traceability_process = dynamodb.Table(table_name)
    try:
        response = table_traceability_process.update_item(
            Key={
                'numFac': num_fac
            },
            UpdateExpression="SET step = :step, pdfCreationStartDate = :v1, pdfCreationDuration = :v2",
            ExpressionAttributeValues={
                ':step': step,
                ':v1': pdf_creation_start_date,
                ':v2': pdf_creation_duration
            },
            ReturnValues="UPDATED_NEW"
        )
    except Exception as e:
        print(e)

def select_document(table_name:str,num_fac:str):
    print("Inicio de consulta factura en la tabla de estados de la DIAN")
    
    try:
        #projection_document_expression = 'path, filename, documentTypeAbbreviation'  # Lista de campos a consultar
        table_dian = dynamodb.Table(table_name)
        response = table_dian.get_item(
            Key={'numFac': num_fac},
        )
        if 'Item' in response:
            return response
        else:
            return None
    except Exception as e:
        print(f"Error al consultar la base de datos: {e}")
        return None

def select_info_email(table_name:str,num_fac:str):
    print("Inicio de consulta factura en la tabla de estados de la DIAN")
    
    try:
        projection_document_expression = '#d'  # Lista de campos a consultar
        table_dian = dynamodb.Table(table_name)
        response = table_dian.get_item(
            Key={'numFac': num_fac},
            ProjectionExpression=projection_document_expression,
            ExpressionAttributeNames={
                '#d': 'data'
            }
        )
        if 'Item' in response:
            return response
        else:
            return None
    except Exception as e:
        print(f"Error al consultar la base de datos: {e}")
        return None

def lambda_handler(event, context):
    environment = context.invoked_function_arn.split(":")[-1]
    #print(body)
    #print(event)
    supplier_id = event["supplierId"]
    num_fac = event["numFac"]

    print(f"##########{num_fac}##########")

    dian_status_table_name = f"{environment}_{supplier_id}_dianStatus"
    send_detail_table_name = f"{environment}_{supplier_id}_sendDetail"
    print(send_detail_table_name)
    #invoice_response = select_document(table_name, num_fac)
    invoice_response = select_info_email(send_detail_table_name, num_fac)
    if invoice_response is None:
        return {
            "message": "No se encontro el documento"
        }
    
    '''
    customer_bucket = f"fe.{environment.lower()}.{customer_tin}"
    path_files = invoice_response["Item"]["path"]
    file_name = invoice_response["Item"]["fileName"]
    customer_id = invoice_response["Item"]["customerId"]
    val_tot_fac = invoice_response["Item"]["amount"]
    document_type_abbreviation = invoice_response["Item"]["documentTypeAbbreviation"]
    #customer_registration_name = invoice_response["Item"]["customerRegistrationName"] #No tengo este dato en la BD (REVISAR SI ES NECESARIO)
    customer_name = invoice_response["Item"]["customerName"]
    supplier_registration_name = invoice_response["Item"]["supplierRegistrationName"] #REVISAR
    customer_electronic_mail = invoice_response["Item"]["customerElectronicMail"] #REVISAR

    #Esta es la tabla que registra los tiempos de creacion del PDF, no aplica
    #traceability_process_table_name = f"{environment}_{customer_tin}_traceabilityProcess"
    #update_traceability_process(traceability_process_table_name,num_fac,3,pdf_creation_start_date,pdf_creation_execution_time_ms)

    send_data = {}
    send_data["supplierId"] = customer_tin
    send_data["customerId"] = customer_id
    send_data["customerBucket"] = customer_bucket
    send_data["valTotFac"] = val_tot_fac
    send_data["numFac"] = num_fac
    send_data["documentTypeAbbreviation"] = document_type_abbreviation
    send_data["customerRegistrationName"] = customer_name
    send_data["customerName"] = customer_name
    send_data["supplierRegistrationName"] = supplier_registration_name  
    send_data["customerElectronicMail"] = customer_electronic_mail
    send_data["filePath"] = path_files
    send_data["fileName"] = file_name
    send_data["subject"] = data["subject"]
    '''

    message = invoice_response["Item"]["data"]
    #message = json.dumps(send_data)
    send_sqs(f"{URL_SQS_SENDEMAIL}{environment}_Email_Send-ondemand-raw-EAP",message)
