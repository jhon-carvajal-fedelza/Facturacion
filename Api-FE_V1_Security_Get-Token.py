import os
import jwt
import json
import boto3
import base64
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

# Inicializar el cliente de DynamoDB
dynamodb = boto3.resource('dynamodb')

# Clave secreta para la firma del token
SECRET_KEY = os.environ.get('secretJWT', '')
print(SECRET_KEY)

result_error = '<RESPCOMANDO><CODIGO>1</CODIGO><MENSAJE>USUARIO Y/O CONTRASEÑA INCORRECTOS.</MENSAJE></RESPCOMANDO>'

#Enviada por siesa
#4ef0507b764d19805808b3b88ff58f678744e6e6

def exist_companyTin(environment:str,company_tin:str):
    # Verificar si el usuario ya existe
    print("Inicio de consulta nit en la tabla de usuarios del API")
    table_name = f"{environment}_userApi"
    table_user = dynamodb.Table(table_name)
    response = table_user.get_item(Key={'userApiId': company_tin})

    if 'Item' in response:
        return True
    else:
        return False

def select_password(environment:str,company_tin:str):
    print("Inicio de consulta password en la tabla de usuarios del API")
    
    try:
        table_name = f"{environment}_userApi"
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

def get_customer_data(environment:str,company_tin:str)->dict:
    """
    Esta función obtiene los datos de la campaña.

    Args:
        campaign_name (str): Nombre de la campana

    Returns:
        dict: Nombre de la campaña
    """
    table_name = f"{environment}_userApi"
    table_user = dynamodb.Table(table_name)
    projection_customer_expression = 'password, activeUser, companyName'  # Lista de campos a consultar

    response_customer = table_user.get_item(
        Key={'userApiId': company_tin},
        ProjectionExpression=projection_customer_expression
    )
    return response_customer

def parse_soap_request(body):
    """
    Parsea el payload SOAP y extrae los campos necesarios.
    """
    print("Inicio de parseo del SOAP")
    try:
        # Parsear el XML SOAP
        soap_root = ET.fromstring(body)
        ns = {"S": "http://schemas.xmlsoap.org/soap/envelope/"}
        tem = {"tem": "http://tempuri.org/"}
        soap_body = soap_root.find("S:Body", ns)
        soap_request = soap_body.find("tem:DevuelveToken",tem)
        process_type = soap_request.find("tem:tipo",tem)
        config = soap_request.find("tem:config",tem).text
        decoded_xml = base64.b64decode(config).decode("utf-8")
        payload_root = ET.fromstring(decoded_xml)
       
        #payload_body = payload_root.find("soap:Body")
        
        
        # Extraer datos
        nit = payload_root.find("NIT").text
        usuario = payload_root.find("USUARIO").text
        password = payload_root.find("PASSWORD").text
        sistema = payload_root.find("SISTEMA").text
        ip = payload_root.find("Ip").text
        #xml_data = request.find("xmlData").text

        return {
            "processType": process_type,
            "nit": nit,
            "usuario": usuario,
            "password": password,
            "sistema": sistema,
            "ip": ip
        }
    except Exception as e:
        raise ValueError(f"Error al parsear el SOAP: {e}")

def generate_token(tin, user, password, system, ip, company):
    """
    Genera un token JWT basado en los datos del usuario.
    """
    print("Inicio de generación del token")
    payload = {
        "tin": tin,
        "user": user,
        "password": password,
        "company": company,
        "system": system,
        "ip": ip,
        "iat": datetime.utcnow(),
        "exp": datetime.utcnow() + timedelta(hours=1)
    }
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")

def create_response(result):
    """
    Crea la respuesta SOAP con el token generado.
    """
    print("Inicio de creación de respuesta SOAP")
    ns_soap = "http://schemas.xmlsoap.org/soap/envelope/"
    ns_tempuri = "http://tempuri.org/"

    ET.register_namespace("s", ns_soap)

    envelope = ET.Element(f"{{{ns_soap}}}Envelope")
    body = ET.SubElement(envelope, f"{{{ns_soap}}}Body")
    devuelve_token_response = ET.SubElement(body, "DevuelveTokenResponse", {"xmlns": "http://tempuri.org/"})
    token_result = ET.SubElement(devuelve_token_response, "DevuelveTokenResult")
    
    token_result.text = result

    # Convertir el árbol XML a una cadena
    xml_response = ET.tostring(envelope, encoding="utf-8")    
    return xml_response

def lambda_handler(event, context):
    """
    Punto de entrada para la Lambda.
    """
    response = ''
    try:
        environment = context.invoked_function_arn.split(":")[-1]
        # Extraer el cuerpo del payload SOAP desde el evento
        body = event.get("body-json", "")      
        print(body)
        # Parsear la solicitud SOAP
        data = parse_soap_request(body)
        code = ""
        message = ""
        token = ""
        nit = data["nit"]
        user = data["usuario"]
        print(f"Usuario: {user}")

        if exist_companyTin(environment,nit):            
            print(f"Nit {nit} encontrado en la tabla de usuarios del API")
            response_customer_data = get_customer_data(environment,nit)
            print(f"Respuesta de la consulta de password: {response_customer_data}")
            isActive = response_customer_data['Item']['activeUser']
            print(f"Usuario activo: {isActive}")
            isBlocked = False  

            if (isActive):
                print("Usuario activo")
                if (isBlocked):
                    print("Usuario bloqueado")                    
                    response = create_response(result_error)
                else:
                    print("Usuario correcto para validar")
                    #validar la contraseña enviada
                    storage_password = response_customer_data['Item']['password']
                    password = data['password'] #posiblemente me toque pasarla a SHA-1
                    #pasar password a SHA-1

                    print(f"Contraseña enviada: {password}")
                    print(f"Contraseña almacenada: {storage_password}")

                    if (storage_password == password):
                        # Generar token
                        print("Contraseña correcta")
                        company = response_customer_data['Item']['companyName']
                        token = generate_token(data["nit"], user, password, data["sistema"], data["ip"], company)
                        print(f"Token: {token}")
                        result = '<RESPCOMANDO><CODIGO>-1</CODIGO><MENSAJE>EXITO</MENSAJE><TOKEN>' + token + '</TOKEN></RESPCOMANDO>'
                        response = create_response(result)
                        print("Proceso ejecutado correctamente")
                    else:
                        print("Contraseña incorrecta") 
                        response = create_response(result_error)                                               
            else:
                print('Usuario o cuenta inactiva, cuenta sin verificar')
                response = create_response(result_error)                

        else:
            print(f"No se encontró el nit enviado en la tabla {user}")
            response = create_response(result_error)            

    except ValueError as e:
        print(f"Error: {e}")
        response = create_response(result_error)
    finally:
        return response