import os
import re
import json
import boto3
import socket
import base64
import zipfile
from zoneinfo import ZoneInfo
import http.client
from lxml import etree
from datetime import datetime, timedelta

SECRET_KEY = os.environ.get('secretJWT','')
POLICY_NAME = os.environ.get('policyName','')
max_retries = 3

REGION = 'us-east-1'
URL_SQS = 'https://sqs.us-east-1.amazonaws.com/381491847136/'
URL_QR_DIAN_DEV = "https://catalogo-vpfe-hab.dian.gov.co/User/SearchDocument?DocumentKey="
URL_QR_DIAN_PROD = "https://catalogo-vpfe.dian.gov.co/User/SearchDocument?DocumentKey="

HOST_DIAN_PROD = "vpfe.dian.gov.co"
HOST_DIAN_DEV = "vpfe-hab.dian.gov.co"
URL_DIAN = "/WcfDianCustomerServices.svc?wsdl"

#https://vpfe-hab.dian.gov.co/WcfDianCustomerServices.svc?wsdl
SOFTWARE_PROVIDER_SCHEME_ID = "9" #os.environ.get('softwareProviderSchemeId','') #Digito verificacion Nit
SOFTWARE_PROVIDER_SCHEME_NAME = "31" #os.environ.get('softwareProviderSchemeName','')
SOFTWARE_PROVIDER_TIN = "901188189" #os.environ.get('softwareProviderTin','') #Nit Fedelza
SOFTWARE_ID = "761eb590-287d-4394-a127-aca5e2680c60"
LLAVE_TECNICA = "fc8eac422eba16e22ffd8c6f94b3f40a6e38162c" #os.environ.get('claveTecnica','')
SOFTWARE_PIN = "11820" #os.environ.get('softwarePin','')
PPP = "123" #os.environ.get('ppp','')
CLAIMED_ROLE = "third party" #os.environ.get('claimedRole','')


TEST_ID = "3b7ba48e-d2e1-47c7-80a2-b071765018dd"
SOFTWARE_PIN = "12345"
SOFTWARE_ID = "8ac926b5-0d37-4308-b75c-a2d3f50c4e2d"


LIST_AGENCY_ID = "6"
SCHEME_AGENCY_ID = "195"
SCHEME_AGENCY_NAME = "CO, DIAN (Dirección de Impuestos y Aduanas Nacionales)"
LIST_AGENCY_NAME = "United Nations Economic Commission for Europe"

# Info DIAN
AUTHORIZATION_PROVIDER_SCHEME_ID = "4"
AUTHORIZATION_PROVIDER_SCHEME_NAME = "31"
AUTHORIZATION_PROVIDER_TIN = "800197268"

BUCKET_NAME_RESOURCES = "resources.fedelza"
CERT_PATH = "config/firm/Certificado2025.pfx"
WSSE_PATH_DEV = "config/firm/Wsse_Dev_V4.xml"
WSSE_PATH_PROD = "config/firm/Wsse_Prod.xml"
CERT_PASSWORD = "YcHPSDrs7q"
POLICY_PATH = f"https://s3.amazonaws.com/{BUCKET_NAME_RESOURCES}/config/policy/politicadefirmav2.pdf"


NAMESPACES = {
    'cac': 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
    'cbc': 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
    'ext': 'urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2',
    'sts': 'dian:gov:co:facturaelectronica:Structures-2-1',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}
#None: 'urn:oasis:names:specification:ubl:schema:xsd:ApplicationResponse-2',
NAMESPACES_AR = {
    'cac': 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
    'cbc': 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
    'ext': 'urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2',
    'sts': 'dian:gov:co:facturaelectronica:Structures-2-1',
    'ds': 'http://www.w3.org/2000/09/xmldsig#',
}


# Inicializar el cliente de DynamoDB
dynamodb = boto3.resource('dynamodb', region_name=REGION)
# Crear el cliente de S3
s3 = boto3.client('s3')

def exist_companyTin(table_name:str,company_tin):
    # Verificar si el usuario ya existe
    print("Inicio de consulta nit en la tabla de usuarios del API")
    '''
    table_user = dynamodb.Table(table_name)
    response = table_user.get_item(Key={'userApiId': company_tin})

    if 'Item' in response:
        return True
    else:
        return False
    '''
    return True

def select_password(table_name:str,company_tin):
    print("Inicio de consulta password en la tabla de usuarios del API")
    
    try:
        table_user = dynamodb.Table(table_name)
        projection_password_expression = 'password, activeUser'  # Lista de campos a consultar
        #projection_password_expression = 'password, activeUser, email, phone'  # Lista de campos a consultar
        response = table_user.get_item(
            Key={'userApiId': company_tin},
            ProjectionExpression=projection_password_expression
        )
        if 'Item' in response:
            return response
        else:
            return None
    except Exception as e:
        print(f"Error al consultar la base de datos: {e}")
        return None
    
def get_invoice(table_name:str,invoice:str):
    print("Inicio de consulta invoice en la tabla de estados DIAN")
    
    try:
        table_dian = dynamodb.Table(table_name)
        response = table_dian.get_item(
            Key={'numFac': invoice},
        )
        if 'Item' in response:
            dian_status = response["Item"]["dianStatus"]
            if dian_status == "Aprobado":
                print("La factura ya fue aprobada por la DIAN") 
                return response
            elif dian_status == "Rechazado":
                print("La factura fue rechazada por la DIAN")
                return response
            elif dian_status == "Timeout":
                print("La factura genero Timeout en la DIAN")
                return response
            else:
                print(f"Factura con estado {dian_status}")
                return None
        else:
            return None
    except Exception as e:
        print(f"Error al consultar la base de datos: {e}")
        return None

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
        print(f"Descargando archivo desde S3...{file_path}")
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

def create_soap_payload(method, wsse_security, dian_url, parameters):
    """
    Crea el payload SOAP con parámetros opcionales.
    
    :param wsse_security: Cadena requerida para el encabezado.
    :param parameters: Diccionario de parámetros opcionales (puede estar vacío o contener valores).
    :return: Cadena XML generada.
    """

    #action_text = f"<wsa:Action>http://wcf.dian.colombia/IWcfDianCustomerServices/{method}</wsa:Action>"
    #to_text = f'<wsa:To wsu:Id="4cb7bad8-fb54-4d2b-b85b-d04bdc447756" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">{dian_url}</wsa:To>'
    #print(action_text)
    #print(to_text)
    date = datetime.utcnow()
    creation_date = date.strftime("%Y-%m-%dT%H:%M:%SZ")
    expiration_Date = (date + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    #creation_date = str(f"{date.isoformat()[:-3]}Z")
    #expiration_Date = str(f"{(date + timedelta(days=1)).isoformat()[:-3]}Z")
    
    # Definir los namespaces
    namespaces = {
        "soap": "http://www.w3.org/2003/05/soap-envelope",
        "wcf": "http://wcf.dian.colombia"
    }
    # Crear el elemento raíz (Envelope)
    envelope = etree.Element(
        "{http://www.w3.org/2003/05/soap-envelope}Envelope", 
        nsmap=namespaces
    )    


    # Asignacion de valores dinamicos
    wsse_security = wsse_security.replace("{{creation_date}}",creation_date)
    wsse_security = wsse_security.replace("{{expiration_Date}}",expiration_Date)
    
    # Crear el namespace wsa solo para el header
    wsa_namespace = {
        "wsa": "http://www.w3.org/2005/08/addressing"
    }

    # Crear el namespace wsu solo para el To
    wsu_namespace = {
        "wsu": "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd"
    }
    
    # Crear el encabezado (Header)
    header = etree.SubElement(envelope, "{http://www.w3.org/2003/05/soap-envelope}Header", nsmap=wsa_namespace)
    
    # Convertir el wsse_security en un elemento XML y añadirlo al encabezado
    wsse_security_element = etree.fromstring(wsse_security)
    header.append(wsse_security_element)
    

    # Añadir Action al Header con el namespace wsa
    action_element = etree.SubElement(header, "{http://www.w3.org/2005/08/addressing}Action")
    action_element.text = f"http://wcf.dian.colombia/IWcfDianCustomerServices/{method}"

    # Añadir To al Header con el namespace wsa
    to_element = etree.SubElement(header, "{http://www.w3.org/2005/08/addressing}To", nsmap= wsu_namespace)
    to_element.set("{http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd}Id", "id-DCD054738C9FFB4DD4173784752305414")
    
    #Cambiar entre ambientes
    to_element.text = "https://vpfe-hab.dian.gov.co/WcfDianCustomerServices.svc"
    #to_element.text = "https://vpfe.dian.gov.co/WcfDianCustomerServices.svc"

    # Crear el cuerpo (Body)
    body = etree.SubElement(envelope, "{http://www.w3.org/2003/05/soap-envelope}Body")
    
    # Crear el método en el cuerpo
    get_xml = etree.SubElement(body, f"{{http://wcf.dian.colombia}}{method}")

    '''
    # Crear el encabezado
    header = ET.SubElement(envelope, "soap:Header", {"xmlns:wsa": "http://www.w3.org/2005/08/addressing"})
    
    wsse_security_element = ET.fromstring(wsse_security)
    header.append(wsse_security_element)


    
    # Crear el cuerpo
    body = ET.SubElement(envelope, "soap:Body")
    get_xml = ET.SubElement(body, f"wcf:{method}")
    '''

    #Agrego los parametros enviados al body (Creando un nodo por cada parametro enviado)
    for key, value in parameters.items():
        element = etree.SubElement(get_xml, f"{{http://wcf.dian.colombia}}{key}")
        element.text = value
    
    # Generar la cadena XML    
    soap_payload = etree.tostring(envelope, encoding="utf-8", pretty_print=False).decode("utf-8")

    return soap_payload

def create_zip(xml, file_name):
    try:
        zip_path = f"/tmp/z{file_name}.zip"
        with zipfile.ZipFile(zip_path, 'w', compression= zipfile.ZIP_DEFLATED) as zip:
            xml = str(xml)
            zip.writestr(f"ff{file_name}.xml", xml.encode())
        file = open(zip_path, 'rb')
        document = base64.b64encode(file.read()).decode('utf-8')
        file.close()
        os.remove(zip_path) 
        return document
    except Exception as ex:
        print("Error comprimiendo el archivo: ",ex)
        return ex

def get_response_dian(host_dian,payload):
    try:
        headers = {
            "content-type": "application/soap+xml"
        }
        conn = http.client.HTTPSConnection(host_dian, timeout=20)
        #conn = http.client.HTTPSConnection(host_dian, timeout=1)
        conn.request("POST", URL_DIAN, payload, headers)
        response = conn.getresponse()
        return response.read()
    except socket.timeout as st:
        print("timeout")
        return None

    except Exception as e:
        print("error e", e)

def insert_traceability_process(table_name:str,num_fac:str,date:str)->None:
    """
    Esta función inserta la trazabilidad del proceso.

    Args:
        table_name (str): Nombre de la tabla para insertar los datos

    Returns:
        None
    """
    table_traceability_process = dynamodb.Table(table_name)

    table_traceability_process.put_item(
        Item={
            'numFac': num_fac,
            'processStartDate': date,
            'step':0
        }
    )

def validate_numbering(table_name:str,customer_id, num_fac):
    numbers = re.sub(r"\D", "", num_fac)

    # Consultar la configuración de numeración en DynamoDB
    table_customer_dian_config = dynamodb.Table(table_name) 
    response = table_customer_dian_config.get_item(Key={'customerId': customer_id})

    # Verificar si existe configuración
    if "Item" not in response:
        return {"valid": False, "message": "No se encontró configuración de numeración"}

    config = response["Item"]

    # Extraer datos de la configuración
    enabled = config["enabled"]["BOOL"]
    initial_range = int(config["initialRange"]["N"])
    final_range = int(config["finalRange"]["N"])
    expiration_date = datetime.strptime(config["numberingExpiration"]["S"], "%Y-%m-%d").date()
    today = datetime.today().date()

    # Validaciones
    if not enabled:
        return {"valid": False, "message": "Numeración deshabilitada"}
    
    if today > expiration_date:
        return {"valid": False, "message": "Numeración expirada"}
    
    if not (initial_range <= numbers <= final_range):
        return {"valid": False, "message": "Número fuera del rango permitido"}

    return {"valid": True, "message": "Numeración válida"}

def insert_dian_status(table_name:str,num_fac:str,date:str,customer_name:str,customer_id:int,cufe:str,qr:str,invoice_date:str,dian_status:str,document_type:str,document_type_abbreviation:str,path:str,file_name:str,prefix:str,amount:str,error_message:str,xml_document_key:str)->None:
    """
    Esta función inserta a la de procesos dian.

    Args:
        table_name (str): Nombre de la tabla para insertar los datos

    Returns:
        None
    """
    table_dian_process = dynamodb.Table(table_name)
    try:
        table_dian_process.put_item(
            Item={
                'numFac': num_fac,
                'date': date,
                'customerName': customer_name,
                'customerId': customer_id,
                'cufe': cufe,
                'qr': qr,
                'invoiceDate': invoice_date,
                'dianStatus': dian_status,
                'documentType': document_type,
                'documentTypeAbbreviation': document_type_abbreviation,
                'path': path,
                'fileName': file_name,
                'prefix': prefix,
                'amount': amount,
                'errorMessage': error_message,
                'xmlDocumentKey': xml_document_key
            }
        )
    except Exception as e:
        print(e)

def insert_dian_audit(table_name:str,num_fac:str,date:str,customer_name:str,customer_id:int,invoice_date:str,dian_status:str)->None:
    """
    Esta función inserta a la de procesos dian.

    Args:
        table_name (str): Nombre de la tabla para insertar los datos

    Returns:
        None
    """
    table_dian_audit = dynamodb.Table(table_name)
    try:
        table_dian_audit.put_item(
            Item={
                'numFac': num_fac,
                'date': date,
                'customerName': customer_name,
                'customerId': customer_id,
                'invoiceDate': invoice_date,
                'dianStatus': dian_status
            }
        )
    except Exception as e:
        print(e)

def format_date(date):
    formatted_date = date.strftime("%Y-%m-%dT%H:%M:%S%z")
    formatted_date = formatted_date[:-2] + ":" + formatted_date[-2:]
    return formatted_date

WSSE = download_file(BUCKET_NAME_RESOURCES,WSSE_PATH_DEV,True,"UTF-8")

def lambda_handler(event, context):
    environment = context.invoked_function_arn.split(":")[-1]
    HOST_DIAN = ""
    response_service = {}
    code = -1
    message = ""
    error_message = ""
    response_message = ""

    if (environment == "Prod"):
        print("################# AMBIENTE PROD #################")        
        HOST_DIAN = HOST_DIAN_PROD   
                      
    else:
        print("################# AMBIENTE DEV #################")
        HOST_DIAN = HOST_DIAN_DEV

        
    try:
        now = datetime.now(ZoneInfo("America/Bogota"))
        # Formatear la fecha y hora según un formato específico
        formatted_date = format_date(now)        
        
        # Extraer Año, Mes y Día
        AAAA = now.strftime("%Y")  # Año
        AA = AAAA[2:4] #Devolver los ultimos 2 digitos
        MM = now.strftime("%m")    # Mes
        DD = now.strftime("%d")    # Día

        # Detectar si viene de SQS o API Gateway
        if "Records" in event and "body" in event["Records"][0]:
            # Es un evento desde SQS
            payload = json.loads(event["Records"][0]["body"])
        else:
            # Es un evento desde API Gateway u otra fuente directa
            payload = event

        print("Payload recibido:", payload)

        # Extraer variables comunes
        tin = payload.get("tin")
        

        table_user_name = f'{environment}_userApi'
        table_customer_dian_config_name = f'{environment}_customerDianConfig' 


        if exist_companyTin(table_user_name,tin):
            print(f"Nit {tin} encontrado en la tabla de usuarios del API")        

            #Proceso consulta numeracion
            account_code = "901188189" #Emisor sin DV (Customer)
            account_codeT = "901188189" #Proveedor tecnologico sin DV (Fedelza)
            SOFTWARE_ID = "fc8eac422eba16e22ffd8c6f94b3f40a6e38162c"
            soap_parameters = {
                "accountCode": account_code,
                "accountCodeT": account_codeT,
                "softwareCode": SOFTWARE_ID
            }
            method = "GetNumberingRange"
            soap_payload = create_soap_payload(method,WSSE,URL_DIAN,soap_parameters)                        
            #Realizar la carga del XML request a S3
            
            print("Inicio de consulta numeracion en la DIAN")
            response_data = get_response_dian(HOST_DIAN, soap_payload)
            if response_data is None:
                #La dian esta generando timeout   
                dian_status = "Timeout"                                
                message = "TimeOut en la DIAN"
                raise ValueError(message)
                                                                                        
            root_response = etree.fromstring(response_data)                        
            print(response_data)                        
            #completos
            
            '''
            namespaces_response_completos = {
                's': "http://www.w3.org/2003/05/soap-envelope",
                'a': "http://www.w3.org/2005/08/addressing",
                'u': "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd",
                'wcf': "http://wcf.dian.colombia",
                'b': "http://schemas.datacontract.org/2004/07/UploadDocumentResponse",
                'i': "http://www.w3.org/2001/XMLSchema-instance"
            }
            '''
            
            namespaces_response = {
                "s": "http://www.w3.org/2003/05/soap-envelope",
                "b": "http://schemas.datacontract.org/2004/07/DianResponse",
                "c": "http://schemas.microsoft.com/2003/10/Serialization/Arrays",
                "": "http://wcf.dian.colombia"  # Espacio de nombres por defecto
            }

            namespaces_response_2 = {
                's': "http://www.w3.org/2003/05/soap-envelope",
                'wcf': "http://wcf.dian.colombia",
                'b': "http://schemas.datacontract.org/2004/07/UploadDocumentResponse"
            }

            #Analizar respuesta de la dian
            path_response = f's:Body/{method}Response/{method}Result/'
            is_valid_element = root_response.find(f'{path_response}b:IsValid', namespaces=namespaces_response)
            status_code_element = root_response.find(f'{path_response}b:StatusCode', namespaces=namespaces_response)
            error_message_element = root_response.find(f'{path_response}b:ErrorMessage', namespaces=namespaces_response)
            status_description_element = root_response.find(f'{path_response}b:StatusDescription', namespaces=namespaces_response)
            status_message_element = root_response.find(f'{path_response}b:StatusMessage', namespaces=namespaces_response) #La Factura electr\xc3\xb3nica SETP990000003, ha sido autorizada.                        
            xml_base64_bytes_element = root_response.find(f'{path_response}b:XmlBase64Bytes', namespaces=namespaces_response) #B64
            xml_file_name_element = root_response.find(f'{path_response}b:XmlFileName', namespaces=namespaces_response)
            xml_document_key_element = root_response.find(f'{path_response}b:XmlDocumentKey', namespaces=namespaces_response)
            application_response = ""

            
            
            response_message = ""
            if status_code_element is not None:
                print("Nodo StatusCode existe en la respuesta de la DIAN")
                status_code = status_code_element.text
                is_valid_text = is_valid_element.text  
                status_description_text = status_description_element.text                      
                status_message_text = status_message_element.text                            
                xml_base64_bytes_text = xml_base64_bytes_element.text
                xml_document_key_text = xml_document_key_element.text

                response_message = status_description_text
                

                print(f"DIAN is valid: {is_valid_text}")
                print(f"DIAN status description: {status_description_text}")
                print(f"DIAN status message: {status_message_text}")

                try:
                    application_response = base64.b64decode(xml_base64_bytes_text).decode()
                    print("Guardar application response")
                    print("OK")
                except Exception as e:                            
                    print(f"Error al decodificar el contenido Base64: {e}")
                    raise ValueError(f"Error al decodificar el contenido Base64: {e}")
                                            
                if status_code == "00":
                    print("StatusCode es igual a 0, la factura fue aprobada")
                    dian_status = "Aprobado"
                    #Enviar a crear el PDF
                    print("Enviando datos para crear PDF")
                    print(response_data)

                elif status_code == "99":
                    print("StatusCode es regla 90, la factura fue aprobada anteriormente")
                    #TODO
                    #Consultar registro en la tabla {tin}_dianProcess
                    #Traer campos path, dianStatus, cufe
                    pass 
                else:                                
                    print("StatusCode es diferente de 0 y diferente de regla 90, la factura NO fue aprobada")
                    dian_status = "Rechazado"
                    print(f"StatusCode: {status_code}")
                    error_messages_element = error_message_element.findall("c:string", namespaces=namespaces_response)
                    code = 1
                    print(response_data)
                    item_str = ""
                    for item in error_messages_element:
                        print(item.text)
                        item_str = item_str + item.text + '\n'
                    error_message = item_str
                    #print(f"DIAN error message: {error_messages_element}")
            else:
                print("No existe el nodo StatusCode dentro de la respuesta de la DIAN")
                print("Respuesta DIAN:")
                print(response_data)

            
            #dianStatus
            #Pendiente
            #Firmado
            #Error
            #ErrorDIAN
            #FirmadoPrueba
            #Cancelado
            #Aprobado
            #Rechazado

            print("Termina validación respuesta DIAN")

        else:
            message = "No se encontró el nit enviado en la tabla de usuarios"
            print(message)
            raise ValueError(message) 
    
    except Exception as e:
        response_message = str(e)

    finally:
        response_service["codigo"] = code
        response_service["mensaje"] = response_message
        response_service["applicationResponse"] = application_response
        response_service["ErrorMessage"] = error_message
        response_service["token"] = ""
        return response_service