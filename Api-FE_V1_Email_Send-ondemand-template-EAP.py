'''
Lambda para realizar el envio de emails con adjunto personalizado
'''
import io
import os
import boto3
import uuid
import json
import time
import zipfile
import sys
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
import re
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

REGION = 'us-east-1'

#Configurar el cliente de DynamoDB
dynamodb = boto3.resource('dynamodb', region_name=REGION)

#Crea un cliente de SES
ses = boto3.client('ses', region_name=REGION)

#Crea el cliente para S3
s3 = boto3.client('s3', region_name=REGION)

'''
Posiblemente necesite crear varias lambdas para los procesos de lectura de las bases de datos del cliente
dependiendo de la cantidad de registros
Debo realizar pruebas:
    con archivos de hasta 50.000 registros
    con archivos de hasta 200.000 registros
    con archivos de hasta 1.000.000 registros
    o verificar hasta cuantos registros puede procesar la memoria mas baja en la lambda sin afectar el tiempo de proceso

Esto implica tener una configuracion para cada cliente en donde se decide por cual lambda se va segun la cantidad de registros que maneja el cliente
'''

'''
Creacion de buckets para cada cliente (En que momento se deben crear?):
document (Se puede definir con el cliente el tiempo de vida de los archivos, dependiendo de si el adjunto se envia online o onfile)
database (Se puede definir tiempo de vida de un mes y posiblemente pasarlo a un S3 glaxier mas barato por unos 3 meses)
El bucket se debe crear con un ciclo de vida definido
'''

def validate_process_detail(table_name:str,part:int)->dict:
    """
    Función encargada de validar el estado de cada parte en la tabla de los detalles.

    Args:
        part (int): Indice de la parte a validar
        
    Returns:
        dict: Informacion de la parte
    """

    table_process_detail = dynamodb.Table(table_name)
    projection_campaign_expression = 'stateProcess, processDetailId'  # Lista de campos a consultar

    response_process_detail = table_process_detail.scan(
        FilterExpression="processId = :value1 and part = :value2",
        ExpressionAttributeValues={":value1": process_id,":value2": part},
        ProjectionExpression=projection_campaign_expression
    )
    return response_process_detail

def insert_sendDetail(table_name:str,num_fac:str,date:str,customer_name:str,customer_id:int,email:str,state_send:str,data:str)->None:
    """
    Esta función inserta la trazabilidad del proceso.

    Args:
        table_name (str): Nombre de la tabla para insertar los datos

    Returns:
        None
    """
    table_send_detail = dynamodb.Table(table_name)

    table_send_detail.put_item(
        Item={
            'numFac': num_fac,
            'date': date,
            'customerName':customer_name,
            'customerId':customer_id,
            'email':email,
            'stateSend':state_send,
            'data':data
        }
    )

def update_traceability_process(table_name:str,num_fac:str,step:int,send_email_start_date:str,send_email_duration:str)->None:
    table_traceability_process = dynamodb.Table(table_name)
    try:
        response = table_traceability_process.update_item(
            Key={
                'numFac': num_fac
            },
            UpdateExpression="SET step = :step, sendEmailStartDate = :v1, sendEmailDuration = :v2",
            ExpressionAttributeValues={
                ':step': step,
                ':v1': send_email_start_date,
                ':v2': send_email_duration
            },
            ReturnValues="UPDATED_NEW"
        )
    except Exception as e:
        print(e)

def get_template(template:str)->dict:
    # Recuperar la plantilla de correo electrónico
    try:
        response_template = ses.get_template(TemplateName=template)
        print("plantilla recuperada correctamente")
   
        return response_template

    except Exception as e:
        print(e)
        print("No se pudo recuperar la plantilla")
    
    #Proceso de recuperacion mediante POST
    '''
    url = "https://api.mailconnect.com.co/v1/Template/Get-template"
    data = {
        "userId": "sdkjfk8hsdf",
        "templateName": template
    }
    response = requests.post(url,data)


    if response.status_code == 200:
        # La solicitud fue exitosa
        jsonResponse = response.json()
        subject = jsonResponse.template.SubjectPart
        html = jsonResponse.template.htmlPart
         
    else:
        # La solicitud falló
        print("statusCode: " + response.status_code)
    ''' 

def download_file(bucket_name:str,file_path:str,decode:bool=False,encode:str="UTF-8"):    
    """
    Descarga un archivo desde un bucket de Amazon S3 y devuelve su contenido.

    Args:
        bucket_name (str): Nombre del bucket de S3 donde se encuentra el archivo.
        file_path (str): Ruta completa del archivo dentro del bucket.
        decode (bool) opcional: Si es True, decodifica el contenido del archivo a una cadena de texto. Por defecto es False.
        encode (str) opcional: Codificación a utilizar para decodificar el contenido si decode es True.El valor predeterminado es "UTF-8".

    Return:
    str or bytes or None
        - Si `decode` es True, devuelve el contenido del archivo como una cadena de texto (str).
        - Si `decode` es False, devuelve el contenido del archivo en formato binario (bytes).
        - Si ocurre un error, devuelve None.

    Excepciones:
    ------------
    Captura cualquier excepción que ocurra durante la descarga e imprime un mensaje de error.

    Usage examples:
    ---------------
    # Descargar archivo en formato binario
    file_content = download_file('my-bucket', 'path/to/file.txt')

    # Descargar y decodificar el archivo como texto
    file_text = download_file('my-bucket', 'path/to/file.txt', decode=True, encode="ISO-8859-1")
    """
    try:
        print(f"Descargando archivo desde S3...")
        s3_object = s3.get_object(Bucket=bucket_name, Key=file_path)
        file_content = s3_object['Body'].read()
        if decode:
            file_string= file_content.decode(encode)
            return file_string
        else:
            return file_content
    except Exception as e:
        print(f"Error al descargar el archivo: {e}")
        return None
    
def save_document(bucket_name,path,file_type,input_file,file_name,prefix):
    """
    Guarda archivos en S3 segun un path, prefijo y nombre entregado.

    Args:
        *args: Una cantidad variable de argumentos que serán concatenados.

    Returns:
        str: Url del objeto en S3.
    """
    try:
        #Revisar: ContentType='application/xml'
        name = f"{prefix}{file_name}.{file_type}"
        key = f'{path}/{file_type}/{name}'             
        s3.put_object(Bucket=bucket_name, Key=key, Body=input_file)
        return str('https://'+ bucket_name +'.s3.amazonaws.com/' + name)
    except Exception as ex:
        raise Exception(f'Problemas al guardar el documento {name}, error: {str(ex)}')

def format_date(date):
    formatted_date = date.strftime("%Y-%m-%dT%H:%M:%S%z")
    formatted_date = formatted_date[:-2] + ":" + formatted_date[-2:]
    return formatted_date

def create_zip(pdf_content, xml_content, file_name):
    try:
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
            # Agregar el XML
            zipf.writestr(f"{file_name}.xml", xml_content)
            # Agregar el PDF
            zipf.writestr(f"{file_name}.pdf", pdf_content)
        
        zip_buffer.seek(0)  # Volver al inicio para lectura
        return zip_buffer.getvalue()  # Retorna los bytes del ZIP
    
    except Exception as ex:
        print("Error comprimiendo los archivos: ", ex)
        return None

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
    configutation_set = f"{environment}-configuration-set-Fedelza"
    status = True
    description = "Campaña enviandose correctamente"
    status_code = 200

    text = ""
    html = ""    

    
    
    try:
        #TODO
        #Validar si el registro se encuentra en la tabla blacklist
        
        # Obtener datos del evento
        body = event["Records"][0]["body"]
        json_body = json.loads(body)
        file_path = json_body["filePath"]
        file_name = json_body["fileName"]
        
        email_creation_start_date = format_date(datetime.now(ZoneInfo("America/Bogota")))
        email_creation_start_time = time.time()
        
        num_fac = json_body["numFac"]
        supplier_id = json_body["supplierId"]
        customer_id = int(json_body["customerId"])
        customer_name = json_body["customerName"]
        val_tot_fac = json_body["valTotFac"]
        attachment_type_abbreviation = "ad"
        supplier_registration_name = json_body["supplierRegistrationName"]
        customer_registration_name = json_body["customerRegistrationName"]
        document_type_abbreviation = json_body["documentTypeAbbreviation"]
        print("Customer: " + supplier_registration_name)
        customer_electronic_mail = json_body["customerElectronicMail"]
        from_email = "facturacion@fedelza.com"
        print("Email: " + customer_electronic_mail)
        headers = ["customerRegistrationName","supplierRegistrationName","valTotFac"]
        template_name = "NotificacionesEstandar"
        #bucket_name = f"fe.{environment}.{supplierId}"
        bucket_name = json_body["customerBucket"]
        
        pdf_path = f"{file_path}/pdf/{document_type_abbreviation}{file_name}.pdf"
        print(pdf_path)
        xml_path = f"{file_path}/xml/{attachment_type_abbreviation}{file_name}.xml"
        print(xml_path)
        pdf_bytes = download_file(bucket_name,pdf_path)
        xml_bytes = download_file(bucket_name,xml_path)
        print("Archivos descargados")
        zip_bytes = create_zip(pdf_bytes, xml_bytes, file_name)
        save_document(bucket_name,file_path,"zip",zip_bytes,file_name,"z")
        
        #Consultar la informacion de la plantilla
        response_template = get_template(template_name)
        print(response_template)
        subject = json_body["subject"]
        print(f"Subject: {subject}")
        
        html = response_template["Template"]["HtmlPart"]
        
        #print(html)
        
        text = response_template["Template"].get('textPart','')
        #print(f"text:{text}")

    

    except Exception as e:
        print(e)
        status = False
        statusCode = 500
        description = "Error no controlado en el servicio"
    else:
        tag_customer_name = customer_registration_name.replace(" ","_")
        default_tags = [{
                "Name":"supplierId",
                "Value":supplier_id
            },
            {
                "Name":"customerName",
                "Value":tag_customer_name
            },
            {
                "Name":"customerId",
                "Value":str(customer_id)
            },
            {
                "Name":"numFac",
                "Value":num_fac
        }]
      
        table_name = f"{environment}_{supplier_id}_sendDetail"

        '''
        attachmentPath = f'{campaign_id}/{doc_name}'
        s3_object = s3.get_object(Bucket=bucket_name, Key=attachmentPath)
        print("consulta adjunto ejecutada correctamente")
        file_content = s3_object['Body'].read()
        file_object = file_content.decode('ISO-8859-1')
        #file_object = BytesIO(file_content)
        '''

        #Reemplazar variables en HTML
        personalized_text = text.replace("{{suplierRegistrationName}}",supplier_registration_name)
        personalized_text = personalized_text.replace("{{customerRegistrationName}}",customer_registration_name)
        personalized_text = personalized_text.replace("{{valTotFac}}",val_tot_fac)
        personalized_text = personalized_text.replace("{{numFac}}",num_fac)
        
        personalized_body = html.replace("{{suplierRegistrationName}}",supplier_registration_name)
        personalized_body = personalized_body.replace("{{customerRegistrationName}}",customer_registration_name)
        personalized_body = personalized_body.replace("{{valTotFac}}",val_tot_fac)
        personalized_body = personalized_body.replace("{{numFac}}",num_fac)
        #print(personalized_text)
        '''
        for field_body in custom_body_fields:
            key = re.sub(replace_pattern, "", field_body)
            value = str(register[key])
            personalized_body = personalized_body.replace(field_body,value)
            personalized_text = personalized_text.replace(field_body,value)
        '''
            
        msg = MIMEMultipart('mixed')
        msg['Subject'] = subject
        
        
        # Add body to email
        msg_body = MIMEMultipart('alternative')
        textpart = MIMEText(personalized_text.encode('utf-8'), 'plain', 'utf-8')
        htmlpart = MIMEText(personalized_body.encode('utf-8'), 'html', 'utf-8')
        msg_body.attach(textpart)
        msg_body.attach(htmlpart)
        msg.attach(msg_body)
        print("Se agrego el body al mensaje")

        #Agregar adjunto Ant
        #part = MIMEApplication(file_object)
        #part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(doc_name))
        #msg.attach(part)
        
        #Agregar adjunto
        part = MIMEApplication(zip_bytes)
        part.add_header('Content-Disposition', 'attachment', filename=f"z{file_name}.zip")
        msg.attach(part)

        #Obtener la fecha y hora actual
        formatted_date = format_date(datetime.now(ZoneInfo("America/Bogota")))    
        
        # Try to send the email.
        try:            
            response = ses.send_raw_email(
                Source=from_email,
                Destinations=[customer_electronic_mail],
                ConfigurationSetName=configutation_set,
                Tags=default_tags,
                RawMessage={'Data': msg.as_string()}
            )            
            
            insert_sendDetail(table_name,num_fac,formatted_date,customer_name,customer_id,customer_electronic_mail,"Procesado",body)
            
            traceability_process_table_name = f"{environment}_{supplier_id}_traceabilityProcess"
            email_creation_end_time = time.time()
            email_creation_execution_time_ms = int((email_creation_end_time - email_creation_start_time) * 1000)
            update_traceability_process(traceability_process_table_name,num_fac,4,email_creation_start_date,email_creation_execution_time_ms)
        except Exception as e:
            insert_sendDetail(table_name,num_fac,formatted_date,customer_name,customer_id,customer_electronic_mail,"Error",body)
            print(e)
                

        

    finally:
        # Respuesta
        response = {
            'status':status,
            'statusCode': status_code,
            'description':description
        }

    return response