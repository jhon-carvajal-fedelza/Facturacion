'''
Lambda para realizar el envio de emails con adjunto personalizado
'''
import io
import os
import boto3
import uuid
import json
import zipfile
import sys
from datetime import datetime, timedelta
import re
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

REGION = 'us-east-1'


#Configurar el cliente de DynamoDB
dynamodb = boto3.resource('dynamodb')

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



table_document = dynamodb.Table('document')
table_campaign = dynamodb.Table('campaign')

def insert_processDetail(process_detail_id,customer_name,registers,part,date,state):
    #debo contar los registros del array data para poner ese valor en el campo total de la tabla {customer}_processDetail
    table_processDetail = dynamodb.Table(f'{customer_name}_processDetail')
    
    # Insertar datos en la tabla de detalle de procesos
    table_processDetail.put_item(
        Item={
            'processDetailId': process_detail_id,
            'processId': process_id,
            'registers': registers,
            'part': part,
            'date': date,
            'stateProcess': state
        }
    )

def validate_process_detail(part:int)->dict:
    """
    Función encargada de validar el estado de cada parte en la tabla de los detalles.

    Args:
        part (int): Indice de la parte a validar
        
    Returns:
        dict: Informacion de la parte
    """

    table_process_detail = dynamodb.Table(f'{customer_name}_processDetail')
    projection_campaign_expression = 'stateProcess, processDetailId'  # Lista de campos a consultar

    response_process_detail = table_process_detail.scan(
        FilterExpression="processId = :value1 and part = :value2",
        ExpressionAttributeValues={":value1": process_id,":value2": part},
        ProjectionExpression=projection_campaign_expression
    )
    return response_process_detail

def insert_sendDetail(processDetailId,customerName,registers,part,date,state):
    #debo contar los registros del array data para poner ese valor en el campo total de la tabla {customer}_processDetail
    tableName = f'{customerName}_processDetail'
    
    # Define los datos que deseas insertar
    data_to_insert = [
        {
            'id': {'N': '1'},
            'nombre': {'S': 'Ejemplo1'},
            # Añade más atributos y sus valores según la estructura de tu tabla
        },
        {
            'id': {'N': '2'},
            'nombre': {'S': 'Ejemplo2'},
            # Añade más atributos y sus valores según la estructura de tu tabla
        }
    ]

    # Realiza la inserción en lotes utilizando el método batch_write_item
    with dynamodb.batch_write_item(RequestItems={tableName: [{'PutRequest': {'Item': item}} for item in data_to_insert]}) as response:
        pass  # La inserción se realiza en el bloque 'with'

    # Verifica si hubo errores en la inserción
    if response.get('UnprocessedItems'):
        print('Hubo elementos no procesados:', response['UnprocessedItems'])
    else:
        print('Todos los elementos se insertaron correctamente.')

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
    
    status = True
    description = "Campaña enviandose correctamente"
    status_code = 200

    subject = ""
    text = ""
    html = ""
    
    #utc_now = datetime.datetime.utcnow()
    #bogota_timezone = timezone('America/Bogota')
    #colombia_now = utc_now.astimezone(bogota_timezone)
    #Obtener la fecha y hora actual
    now = datetime.utcnow()
    colombia_time = now - timedelta(hours=5)
    # Formatear la fecha y hora según un formato específico
    formatted_date = colombia_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'
    print(formatted_date)
    process_detail_id = str(uuid.uuid4())
    
    
    try:

        # Obtener datos del evento
        body = event["Records"][0]["body"]
        json_body = json.loads(body)
        file_path = json_body["filePath"]
        file_name = json_body["fileName"]
        subject = json_body["subject"]
        
        
        num_fac = json_body["numFac"]
        supplier_Id = json_body["supplierId"]
        val_tot_fac = json_body["valTotFac"]
        document_type_abbreviation = json_body["documentTypeAbbreviation"]
        attr_attach= "ad"
        supplier_registration_name = json_body["supplierRegistrationName"]
        customer_registration_name = json_body["customerRegistrationName"]
        print("Customer" + customer_name)
        customer_electronic_mail = json_body["customerElectronicMail"]
        from_email = "facturacion@fedelza.com"
        headers = ["customerRegistrationName","supplierRegistrationName","valTotFac"]
        template_name = "TemplateEstandar"
        bucket_name = json_body["customerBucket"]

        traceability_process_table_name = f"{supplier_id}_traceabilityProcess"
        send_detail_table_name = f"{supplier_id}_sendDetail"
        send_status_table_name = f"{supplier_id}_sendStatus"
        black_list_table_name = f"{supplier_id}_blacklist"
        
        pdf_path = f"{file_path}/{document_type_abbreviation}{file_name}.pdf"
        xml_path = f"{file_path}/{attr_attach}{file_name}.xml"
        pdf_bytes = download_file(bucket_name,pdf_path)
        xml_bytes = download_file(bucket_name,xml_path)
        zip_bytes = create_zip(pdf_bytes, xml_bytes, file_name)
        save_document(bucket_name,file_path,"zip",zip_bytes,file_name,"z")
        #Validar el estado de la parte a procesar (Si se encuentra "Realizando envios" puede ser un error de mensajes duplicados)
        #Debo generar error para evitar realizar envios duplicados
        response_process_detail = validate_process_detail(part)
        if response_process_detail['Items']:
            state = response_process_detail['Items'][0]["stateProcess"]
            if (state == "Realizando envios"):
                print(f"La parte {part} del proceso {process_id} ya se encuentra realizando los envios")
                print(f"El id: {process_id} se encuentra en estado {state}")
                raise ValueError("La parte ya ha sido procesada")
        print("Inicia actualización del estado a Realizando envios")
        #insert_processDetail(process_detail_id,customer_name,registers,part,formatted_date,"Realizando envios")

        #Consultar la informacion de la plantilla
        response_template = get_template(template_name)
        
        
        html = response_template["Template"]["HtmlPart"]
        print(html)
        
        text = response_template["Template"].get('textPart','')
        print(f"text:{text}")

        
        table_sendDetail = dynamodb.Table(f'{customer_name}_sendDetail')

    except Exception as e:
        print(e)
        status = False
        statusCode = 500
        description = "Error no controlado en el servicio"
    else:
        #El parametro DefaultTemplateData  lo puedo usar para el reemplazo de las url para adjuntos ONLINE
        #El cliente solo necesita cargar el adjunto a S3 (Mediante la creacion de la campa;a)
        #Despues debe poner en la plantilla de email un campo de reemplazo o varios campos de reemplazo asi:
        #{Adjunto1}, {Adjunto2}, {AdjuntoN}
        #Al crear la campa;a, el front debe validar que si exista la cantidad X de adjuntos segun los datos cargados a S3
        #Si son varios adjuntos no pueden ser de diferente tipo (ONLINE; ONFILE)
        #Por aca solo debrian pasar los email con adjunto ONFILE, los ONLINE deben ir en email marketing

        default_tags = [{
                "Name":"supplier",
                "Value":supplier_registration_name
            },
            {
                "Name":"customer",
                "Value":customer_registration_name
            },
            {
                "Name":"processId",
                "Value":process_id
        }]
      


        
        attachmentPath = f'{campaign_id}/{doc_name}'
        s3_object = s3.get_object(Bucket=bucket_name, Key=attachmentPath)
        print("consulta adjunto ejecutada correctamente")
        file_content = s3_object['Body'].read()
        file_object = file_content.decode('ISO-8859-1')
        #file_object = BytesIO(file_content)

        #Reemplazar variables en HTML
        personalized_text = text.replace("{{supplierRegistrationName}}",supplier_registration_name)
        personalized_text = text.replace("{{customerRegistrationName}}",customer_registration_name)
        personalized_text = text.replace("{{supplierRegistrationName}}",val_tot_fac)
        personalized_text = text.replace("{{numFac}}",num_fac)
        
        personalized_body = body.replace("{{supplierRegistrationName}}",supplier_registration_name)
        personalized_body = body.replace("{{customerRegistrationName}}",customer_registration_name)
        personalized_body = body.replace("{{supplierRegistrationName}}",val_tot_fac)
        personalized_body = body.replace("{{numFac}}",num_fac)
        print(personalized_text)
        '''
        for field_body in custom_body_fields:
            key = re.sub(replace_pattern, "", field_body)
            value = str(register[key])
            personalized_body = personalized_body.replace(field_body,value)
            personalized_text = personalized_text.replace(field_body,value)
        '''
            
        msg = MIMEMultipart('mixed')
        msg['Subject'] = personalized_subject
        
        
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

        
        # Try to send the email.
        try:
            response = ses.send_raw_email(
                Source=from_email,
                Destinations=[customer_electronic_mail],
                ConfigurationSetName="default",
                Tags=default_tags,
                RawMessage={'Data': msg.as_string()}
            )
            '''
            response->
            {
                'MessageId': 'string'
            }
            '''
        except Exception as e:
            #Si alguno de los envios no se puede realizar debo enviarlo a una cola para darle manejo
            print(e)
                

        

    finally:
        # Respuesta
        response = {
            'status':status,
            'statusCode': status_code,
            'description':description
        }

    return response