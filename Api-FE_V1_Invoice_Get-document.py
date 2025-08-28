import json
import boto3

REGION = 'us-east-1'
# Inicializar el cliente de DynamoDB
dynamodb = boto3.resource('dynamodb', region_name=REGION)
# Crear el cliente de S3
s3 = boto3.client('s3')

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

def lambda_handler(event, context):
    print(event)
    abbreviation = ""
    supplier_id = event.get('identificationNumber')
    num_fac = event.get('numFac')
    document_type = event.get('documentType')
    table_name = f"{supplier_id}_dianStatus"
    invoice_response = select_document(table_name, num_fac)
    if invoice_response is None:
        return {
            "message": "No se encontro el documento"
        }
    path_files = invoice_response["Item"]["path"]
    file_name = invoice_response["Item"]["fileName"]
    document_type_abbreviation = invoice_response["Item"]["documentTypeAbbreviation"]
    bucket_name = f"fe.{supplier_id}"

    if (document_type == "pdf"):
        abbreviation = document_type_abbreviation
    elif (document_type == "xml"):
        abbreviation = "ad"

    print(f"Creando url prefirmada de {document_type} para {file_name}")
    document_key = f"{path_files}/{document_type}/{abbreviation}{file_name}.{document_type}"
    generate_presigned_url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket_name, "Key": document_key},
        ExpiresIn=3600
    )
    return {
        "message": "Documento encontrado",
        "url": generate_presigned_url
    }
