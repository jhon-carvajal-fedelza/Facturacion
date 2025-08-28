import os
import re
import jwt
import json
import time
import boto3
import chilkat
import socket
import xmlsig
import base64
import zipfile
import hashlib
from decimal import Decimal, ROUND_HALF_UP
from dateutil import tz
from zoneinfo import ZoneInfo
import http.client
from lxml import etree
from botocore.config import Config
from jwt.exceptions import PyJWTError
from datetime import datetime, timedelta
from xades.policy import GenericPolicyId
from xades import XAdESContext, template, utils
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.x509 import Certificate
#from tenacity import retry, stop_after_attempt, wait_exponential
from jwt import ExpiredSignatureError, InvalidTokenError, DecodeError, InvalidSignatureError

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

NAMESPACES_ATTACH = {
    None: 'urn:oasis:names:specification:ubl:schema:xsd:AttachedDocument-2',
    'cac': 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
    'cbc': 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
    'ccts': 'urn:un:unece:uncefact:data:specification:CoreComponentTypeSchemaModule:2',
    'ds': 'http://www.w3.org/2000/09/xmldsig#',
    'ext': 'urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2',
    'xades': 'http://uri.etsi.org/01903/v1.3.2#',
    'xades141': 'http://uri.etsi.org/01903/v1.4.1#'
}
CAC_NS = "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2"
CBC_NS = "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2"
EXT_NS = "urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2"

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

XML_PATHS = {
    "invoice": {
        "id": "cbc:ID",  # Número de la factura
        "issue_date": "cbc:IssueDate",  # Fecha de emisión
        "issue_time": "cbc:IssueTime", #Hora de emisión
        "due_date": "cbc:DueDate",  # Fecha de vencimiento
        "currency": "ext:UBLExtensions/ext:UBLExtension[2]/ext:ExtensionContent/CustomTagGeneral/TotalesCop/MonedaCop",  # Moneda
        "trm": "ext:UBLExtensions/ext:UBLExtension[2]/ext:ExtensionContent/CustomTagGeneral/TotalesCop/FctConvCop",  # trm
        "invoiceReferenceId": "cac:BillingReference/cac:InvoiceDocumentReference/cbc:ID",  # Referencia a la factura anterior
        "invoiceReferenceDate": "cac:BillingReference/cac:InvoiceDocumentReference/cbc:IssueDate",  # Fecha de la factura anterior
        "invoiceUuid": "cac:BillingReference/cbc:InvoiceDocumentReference/cbc:UUID",  # UUID de la factura anterior
        "authorized": "pendiente",
        "noteType": "pendiente",
        "seller1": "cbc:Note[11]", #Nota 11
        "seller2": "cbc:Note[12]", #Nota 12
        "allowance1": "cbc:Note[5]", #Nota 5
        "allowance2": "cbc:Note[6]", #Nota 6
        "taxExclusiveAmount1": "cbc:Note[4]", #Nota 4
        "taxExclusiveAmount2": "cbc:Note[5]", #Nota 5
        "expires": "cbc:Expires",
        "term1": "cbc:Note[23]", #Nota 23
        "term2": "cbc:Note[24]", #Nota 24
        "order_reference": "cac:OrderReference/cbc:ID",
        "purchace_order": "cbc:PurchaseOrder",
        "payment_id": "cac:PaymentMeans/cbc:ID",  # Forma de pago
        "totalInWords1": "cbc:Note[3]", #Nota 3
        "totalInWords2": "cbc:Note[4]", #Nota 4
        "concept": "cbc:Note[1]", #Nota 1
        "invoice_authorization": "ext:UBLExtensions/ext:UBLExtension[1]/ext:ExtensionContent/sts:DianExtensions/sts:InvoiceControl/sts:InvoiceAuthorization",
        "authorization_start_period": "ext:UBLExtensions/ext:UBLExtension[1]/ext:ExtensionContent/sts:DianExtensions/sts:InvoiceControl/sts:AuthorizationPeriod/cbc:StartDate",
        "authorization_end_period": "ext:UBLExtensions/ext:UBLExtension[1]/ext:ExtensionContent/sts:DianExtensions/sts:InvoiceControl/sts:AuthorizationPeriod/cbc:EndDate",
        "authorization_invoices_prefix": "ext:UBLExtensions/ext:UBLExtension[1]/ext:ExtensionContent/sts:DianExtensions/sts:InvoiceControl/sts:AuthorizedInvoices/sts:Prefix",
        "authorization_invoices_from": "ext:UBLExtensions/ext:UBLExtension[1]/ext:ExtensionContent/sts:DianExtensions/sts:InvoiceControl/sts:AuthorizedInvoices/sts:From",
        "authorization_invoices_to": "ext:UBLExtensions/ext:UBLExtension[1]/ext:ExtensionContent/sts:DianExtensions/sts:InvoiceControl/sts:AuthorizedInvoices/sts:To",
        "profile_execution_id": "cbc:ProfileExecutionID",
        "paylable_amount": "cbc:LegalMonetaryTotal/cbc:PayableAmount",  # Total a pagar
        "payment_mean_code": "cac:PaymentMeans/cbc:PaymentMeansCode"
    },
    "supplier": {
        "name": "cac:AccountingSupplierParty/cac:Party/cac:PartyName/cbc:Name",  # Nombre del proveedor
        "id": "cac:AccountingSupplierParty/cac:Party/cac:PartyTaxScheme/cbc:CompanyID",  # NIT o identificación
        "address": "cac:AccountingSupplierParty/cac:Party/cac:PhysicalLocation/cac:Address/cac:AddressLine/cbc:Line",  # Dirección
        "city": "cac:AccountingSupplierParty/cac:Party/cac:PhysicalLocation/cac:Address/cbc:CityName",  # Ciudad
        "country": "cac:AccountingSupplierParty/cac:Party/cac:PhysicalLocation/cac:Address/cbc:CountrySubentity",  # Departamento
        "email": "cac:AccountingSupplierParty/cac:Party/cac:Contact/cbc:ElectronicMail",  # Correo electrónico
        "telephone": "cac:AccountingSupplierParty/cac:Party/cac:Contact/cbc:Telephone",  # Teléfono
        "registration_name": "cac:AccountingSupplierParty/cac:Party/cac:PartyTaxScheme/cbc:RegistrationName", #Razon social emisor
        "tax_level_code": "cac:AccountingSupplierParty/cac:Party/cac:PartyTaxScheme/cbc:TaxLevelCode",
        "tax_id": "cac:AccountingSupplierParty/cac:Party/cac:PartyTaxScheme/cbc:ID",
        "tax_name": "cac:AccountingSupplierParty/cac:Party/cac:PartyTaxScheme/cbc:Name",
        "industry_classification_code": "cac:AccountingSupplierParty/cac:Party/cbc:IndustryClassificationCode" #Actividad economica
    },
    "customer": {
        "name": "cac:AccountingCustomerParty/cac:Party/cac:PartyName/cbc:Name",  # Nombre del cliente
        "id": "cac:AccountingCustomerParty/cac:Party/cac:PartyTaxScheme/cbc:CompanyID",  # Identificación
        "address": "cac:AccountingCustomerParty/cac:Party/cac:PhysicalLocation/cac:Address/cac:AddressLine/cbc:Line",  # Dirección
        "city": "cac:AccountingCustomerParty/cac:Party/cac:PhysicalLocation/cac:Address/cbc:CityName",  # Ciudad
        "country": "cac:AccountingCustomerParty/cac:Party/cac:PhysicalLocation/cac:Address/cbc:CountrySubentity",  # Departamento
        "email": "cac:AccountingCustomerParty/cac:Party/cac:Contact/cbc:ElectronicMail",  # Correo electrónico
        "neighborhood": "cac:AccountingCustomerParty/cac:Party/cac:Contact/cbc:Neighborhood",
        "telephone": "cac:AccountingCustomerParty/cac:Party/cac:Contact/cbc:Telephone",  # Teléfono
        "registration_name": "cac:AccountingCustomerParty/cac:Party/cac:PartyTaxScheme/cbc:RegistrationName", #Nombre cliente
        "tax_level_code": "cac:AccountingCustomerParty/cac:Party/cac:PartyTaxScheme/cbc:TaxLevelCode",
        "tax_id": "cac:AccountingCustomerParty/cac:Party/cac:PartyTaxScheme/cbc:ID",
        "tax_name": "cac:AccountingCustomerParty/cac:Party/cac:PartyTaxScheme/cbc:Name",
    },
    "legal_monetary_total": {
        "line_extension_amount": "cac:LegalMonetaryTotal/cbc:LineExtensionAmount",  # total bruto
        "allowance_total_amount": "cac:LegalMonetaryTotal/cbc:AllowanceTotalAmount",   # descuento
        "advance_amount": "cac:LegalMonetaryTotal/cbc:AdvanceAmount",                 # anticipo
        "gift_amount": "cac:LegalMonetaryTotal/cbc:GiftAmount",                       # obsequio
        "tax_exclusive_amount": "cac:LegalMonetaryTotal/cbc:TaxExclusiveAmount",        # total
        "payable_amount":"cac:LegalMonetaryTotal/cbc:PayableAmount"
    },
    "tax_total": {
        "tax_total": "cac:TaxTotal",
        "taxable_amount": "cac:TaxSubtotal/cbc:TaxableAmount",  # Base gravable
        "tax_amount": "cac:TaxSubtotal/cbc:TaxAmount",  # Valor del impuesto
        "tax_category": "cac:TaxSubtotal/cac:TaxCategory/cac:TaxScheme/cbc:Name",  # Tipo de impuesto (ej. IVA)
        "tax_percentage": "cac:TaxSubtotal/cac:TaxCategory/cbc:Percent",  # Porcentaje del impuesto        
        "tax_scheme": "cac:TaxSubtotal/cac:TaxCategory/cac:TaxScheme/cbc:ID",
        "imp_tax_amount": "cbc:amount",
        "impoconsumo_amount": "cac:TaxSubtotal/cbc:TaxAmount",       # impoconsumo
        "bag_tax_amount": "cbc:BagTaxAmount"                            # impuesto bolsa
    },
    "tax_breakdown": {
        "iva": "cbc:TaxBreakdown/cbc:TaxAmount",                                      # IVA (segundo bloque)
        "impoconsumo": "cbc:TaxBreakdown/cbc:ImpoconsumoAmount",                      # impoconsumo (segundo bloque)
        "bag_tax": "cbc:TaxBreakdown/cbc:BagTaxAmount",                               # impuesto bolsa (segundo bloque)
        "gift": "cbc:TaxBreakdown/cbc:GiftAmount",                                    # obsequio (segundo bloque)
        "advance": "cbc:TaxBreakdown/cbc:AdvanceAmount",                              # anticipo (segundo bloque)
        "total": "cbc:TaxBreakdown/cbc:TaxInclusiveAmount"                            # total (segundo bloque)
    }, 
    "items": {
        "lines": "cac:InvoiceLine",  # Nodo raíz para las líneas de detalle
        "id": "cac:Item/cac:StandardItemIdentification/cbc:ID",
        "description": "cac:Item/cbc:Description",  # Descripción del producto/servicio
        "unit_of_measure": "cbc:Note[5]", #Nota 5
        "quantity": "cbc:InvoicedQuantity",  # Cantidad
        "unit_price": "cac:Price/cbc:PriceAmount",  # Precio unitario
        "discount_percentage": "cac:AllowanceCharge/cbc:MultiplierFactorNumeric",  # % Descuento
        "tax_total": "cac:TaxTotal",
        "lote": "cbc:Note[24]", #Nota 24
        "free_charge_indicator": "cbc:FreeOfChargeIndicator",
        "line_extension_amount": "cbc:LineExtensionAmount",  # Valor total
        "discount_amount": "cac:AllowanceCharge/cbc:Amount",  # valor Descuento
        "taxTotal": {
            "tax_category": "cac:TaxSubtotal/cac:TaxCategory/cac:TaxScheme/cbc:Name",  # Tipo de impuesto (ej. IVA)
            "tax_id": "cac:TaxSubtotal/cac:TaxCategory/cac:TaxScheme/cbc:ID",
            "tax_percentage": "cac:TaxSubtotal/cac:TaxCategory/cbc:Percent",  # Porcentaje de iva
            "impoconsumo_amount": "cac:TaxSubtotal/cbc:TaxAmount",       # impoconsumo
            "taxable_amount": "cac:TaxSubtotal/cbc:TaxableAmount",  # Base gravable
            "tax_amount": "cac:TaxSubtotal/cbc:TaxAmount",  # Valor del impuesto
        }
    },
    "payment": {
        "means": "cac:PaymentMeans/cac:PaymentMeansCode",  # Medio de pago
        "payee_account": "cac:PaymentMeans/cac:PayeeFinancialAccount/cbc:ID",  # Cuenta del receptor
        "payment_due_date": "cac:PaymentMeans/cbc:PaymentDueDate",  # Fecha de vencimiento
    },
    "legal": {
        "ubl_version": "cbc:UBLVersionID",  # Versión del estándar UBL
        "profile_id": "cbc:ProfileID",  # Tipo de factura (ej. factura electrónica)
    }
}  

# Inicializar el cliente de DynamoDB
dynamodb = boto3.resource('dynamodb', region_name=REGION)
# Crear el cliente de S3
s3 = boto3.client('s3')
# Configurar el cliente de SQS
sqs = boto3.client('sqs', region_name=REGION)

def is_dian_available(table_name:str):
    table_status = dynamodb.Table(table_name)
    response = table_status.get_item(Key={'serviceName': 'DIAN'})
    item = response.get('Item', {})
    status = item.get('status', 'UP')

    if status == 'DOWN' and retry_after:
        return False
    return True

def update_service_status(table_name:str,status:str,date:str):
    table_status = dynamodb.Table(table_name)
    try:
        response = table_status.update_item(
            Key={
                'serviceName': 'DIAN'
            },
            UpdateExpression="SET status = :s, lastChecked = :lc",
            ExpressionAttributeValues={
                ':s': status,
                ':lc': date
            },
            ReturnValues="UPDATED_NEW"
        )
    except Exception as e:
        print(e)

def exist_companyTin(table_name:str,company_tin):
    # Verificar si el usuario ya existe
    print("Inicio de consulta nit en la tabla de usuarios del API")
    table_user = dynamodb.Table(table_name)
    response = table_user.get_item(Key={'userApiId': company_tin})

    if 'Item' in response:
        return True
    else:
        return False

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

def firm_document(xml, cert, password_cert, claimed_role, date_time):
    try:
        root = etree.fromstring(xml)

        signature_id = utils.get_unique_id()
        signature = xmlsig.template.create(
            xmlsig.constants.TransformInclC14N,
            xmlsig.constants.TransformRsaSha256,
            signature_id,
        )
    except Exception as e:
        print(f"Error al firmar el documento: {e}")
        return None

    try:
        #Agregar referencia con transform 'enveloped'
        ref = xmlsig.template.add_reference(
            signature, xmlsig.constants.TransformSha256, uri="", name=signature_id + "-ref0"
        )        
        xmlsig.template.add_transform(ref, xmlsig.constants.TransformEnveloped)

        #Agregar referencia 2
        
        xmlsig.template.add_reference(
            signature, xmlsig.constants.TransformSha256, uri="#" + signature_id + "-ref0"
        )

        xmlsig.template.add_reference(
            signature, xmlsig.constants.TransformSha256, uri="#" + signature_id + "-ref0"
        )
        
        
    except Exception as e:
        print(f"Error al firmar el documento2: {e}")
        return None

    ki = xmlsig.template.ensure_key_info(signature, name=signature_id + "-keyinfo")
    x509_data = xmlsig.template.add_x509_data(ki)
    x509_certificate = xmlsig.template.x509_data_add_certificate(x509_data)
    xmlsig.template.add_key_value(ki)
    qualifying = template.create_qualifying_properties(
        signature, name=utils.get_unique_id(), etsi='xades'
    )

    props = template.create_signed_properties(qualifying, name=signature_id + "-signedprops", datetime= date_time)
    # Additional data for signature
    template.add_claimed_role(props, claimed_role)
    
    try:
        certificate = pkcs12.load_pkcs12(cert, password_cert)

        private_key, cert, additional_certs = pkcs12.load_key_and_certificates(cert, password_cert)
        # Verificar que haya un certificado
        if cert:
            fecha_vencimiento = cert.not_valid_after
            print("El certificado vence el:", fecha_vencimiento)
        else:
            print("No se encontró un certificado dentro del archivo.")

    except Exception as e:
        print(f"Error al cargar el certificado: {e}")
        return None
    
    try:
        ubl_extensions = root.find('.//ext:UBLExtensions', namespaces=NAMESPACES)
        if ubl_extensions is None:
            print("No se encontró el nodo ext:UBLExtensions en el XML.")
            return None
        # Crear el nodo UBLExtension para la firma
        ubl_firm = etree.SubElement(ubl_extensions, '{urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2}UBLExtension')
        extension_content_firm = etree.SubElement(ubl_firm, '{urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2}ExtensionContent')

    except Exception as e:
        print(f"Error al insertar nodos de firma en el XML: {e}")
        return None
    root.append(signature)
    
    try:
        policy = GenericPolicyId(
            "https://s3.amazonaws.com/resources.fedelza/config/policy/politicadefirmav2.pdf",
            "Política de firma para facturas electrónicas de la República de Colombia",
            xmlsig.constants.TransformSha256
        )
        ctx = XAdESContext(
            policy,
            [certificate.cert.certificate]
        )
        ctx.load_pkcs12((certificate.key, certificate.cert.certificate))
        ctx.sign(signature)
    except Exception as e:
        print(f"Error durante el proceso de firma: {e}")
        return None

    try:
        extension_content_firm.append(signature)
        #
    except Exception as e:
        print(f"Error al insertar la firma en el XML: {e}")
        return None
    
    xml_end = etree.tostring(root, xml_declaration = True, encoding="UTF-8", standalone=False)
    print("Termina proceso de firmado")
    return xml_end.decode("UTF-8").replace(
        "<?xml version='1.0' encoding='UTF-8' standalone='no'?>", 
        '<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
    )

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

def generate_sha384_hash(*args):
    """
    Genera un hash SHA-384 para datos como el CUFE-CUDE o software_security_code a partir de la concatenación de campos variables.

    Args:
        *args: Una cantidad variable de argumentos que serán concatenados.

    Returns:
        str: El hash SHA-384 en formato hexadecimal.
    """
    # Convertir todos los argumentos a cadenas y concatenarlos
    concatenated = ''.join(map(str, args))
    # Generar el hash SHA-384
    hash_object = hashlib.sha384(concatenated.encode('utf-8'))
    return hash_object.hexdigest()

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

    '''
    for attempt in range(max_retries + 1):  # Se intenta 1 vez + 2 reintentos
        try:
            conn = http.client.HTTPSConnection(host_dian, timeout=20)
            conn.request("POST", URL_DIAN, payload, headers)
            response = conn.getresponse()
            return response.read()
        except socket.timeout:
            print(f"Intento {attempt + 1}: Timeout. Reintentando...")
            if attempt < max_retries:
                time.sleep(4)  # Pequeña espera antes de reintentar
            else:
                print("Se alcanzó el máximo de intentos por timeout.")
                return None
        except Exception as e:
            print("Error:", e)
            return None
    '''

def get_data(root, parent_node, node, mandatory=False, default=""):
    try:
        value = root.find(XML_PATHS[parent_node][node], namespaces=NAMESPACES)
        if value is not None:
            text_value = value.text if value.text is not None else ""
            return text_value
        else:
            if mandatory:
                exit(f"Error: {node} es obligatorio")
            else:
                return default
    except KeyError:
        print(f"Ruta no encontrada para {node}")
        return exit(f"Error: Ruta no encontrada para {node}") if mandatory else ""

def create_attach_document(xml_firm,application_response,num_fac,issue_date,issue_time,issue_date_ar,issue_time_ar,attr_uuid,unique_code_hash,profile_execution_id,validation_result_code,supplier_id,supplier_registration_name,supplier_tax_level_code,supplier_tax_id,supplier_tax_name,customer_id,customer_registration_name,customer_tax_level_code,customer_tax_id,customer_tax_name,supplier_scheme_name,supplier_scheme_id,customer_scheme_name,customer_scheme_id):
    ##REALIZAR CREACION DEL ATTACH DOCUMENT
    
    root_ad = etree.Element("AttachedDocument",nsmap=NAMESPACES_ATTACH)
    ubl_extensions = etree.SubElement(root_ad, f"{{{EXT_NS}}}UBLExtensions")
    #ubl_extension = etree.SubElement(ubl_extensions, f"{{{EXT_NS}}}UBLExtension")
    #extension_content = etree.SubElement(ubl_extension, f"{{{EXT_NS}}}ExtensionContent")

    etree.SubElement(root_ad, f"{{{CBC_NS}}}UBLVersionID").text = "UBL 2.1"
    etree.SubElement(root_ad, f"{{{CBC_NS}}}CustomizationID").text = "Documentos adjuntos"
    etree.SubElement(root_ad, f"{{{CBC_NS}}}ProfileID").text = "Factura Electrónica de Venta"
    etree.SubElement(root_ad, f"{{{CBC_NS}}}ProfileExecutionID").text = profile_execution_id

    now = datetime.now(ZoneInfo("America/Bogota"))
    issue_date = now.strftime("%Y-%m-%d")
    issue_time = now.strftime("%H:%M:%S")
    issue_time = issue_time + "-05:00"

    etree.SubElement(root_ad, f"{{{CBC_NS}}}ID").text = num_fac
    etree.SubElement(root_ad, f"{{{CBC_NS}}}IssueDate").text = issue_date
    etree.SubElement(root_ad, f"{{{CBC_NS}}}IssueTime").text = issue_time

    etree.SubElement(root_ad, f"{{{CBC_NS}}}DocumentType").text = "Contenedor de Factura Electrónica"
    etree.SubElement(root_ad, f"{{{CBC_NS}}}ParentDocumentID").text = num_fac                            

    #Creación de Party (Emisor)
    sender_party = etree.SubElement(root_ad, f"{{{CAC_NS}}}SenderParty")
    sender_party_tax_scheme = etree.SubElement(sender_party, f"{{{CAC_NS}}}PartyTaxScheme")
    etree.SubElement(sender_party_tax_scheme, f"{{{CBC_NS}}}RegistrationName").text = supplier_registration_name

    supplier_company_id = etree.SubElement(sender_party_tax_scheme, f"{{{CBC_NS}}}CompanyID", schemeAgencyID="195", schemeAgencyName=SCHEME_AGENCY_NAME,schemeID=supplier_scheme_id,schemeName=supplier_scheme_name)
    supplier_company_id.text = supplier_id
    supplier_tax_level_code_element = etree.SubElement(sender_party_tax_scheme, f"{{{CBC_NS}}}TaxLevelCode", listName="No aplica")

    supplier_tax_level_code_element.text = supplier_tax_level_code
    sender_tax_scheme = etree.SubElement(sender_party, f"{{{CAC_NS}}}TaxScheme")
    etree.SubElement(sender_tax_scheme, f"{{{CBC_NS}}}Id").text = supplier_tax_id
    etree.SubElement(sender_tax_scheme, f"{{{CBC_NS}}}Name").text = supplier_tax_name
    
    #Creación de Party (Receptor)
    receiver_party = etree.SubElement(root_ad, f"{{{CAC_NS}}}ReceiverParty")
    receiver_party_tax_scheme = etree.SubElement(receiver_party, f"{{{CAC_NS}}}PartyTaxScheme")
    etree.SubElement(receiver_party_tax_scheme, f"{{{CBC_NS}}}RegistrationName").text = customer_registration_name
    customer_company_id = etree.SubElement(receiver_party_tax_scheme, f"{{{CBC_NS}}}CompanyID", schemeAgencyID="195", schemeAgencyName=SCHEME_AGENCY_NAME,schemeID=customer_scheme_id,schemeName=customer_scheme_name)
    customer_company_id.text = customer_id
    customer_tax_level_code_element = etree.SubElement(receiver_party_tax_scheme, f"{{{CBC_NS}}}TaxLevelCode", listName="No aplica")
    customer_tax_level_code_element.text = customer_tax_level_code
    receiver_tax_scheme = etree.SubElement(receiver_party, f"{{{CAC_NS}}}TaxScheme")
    etree.SubElement(receiver_tax_scheme, f"{{{CBC_NS}}}Id").text = customer_tax_id
    etree.SubElement(receiver_tax_scheme, f"{{{CBC_NS}}}Name").text = customer_tax_name

    #Adjuntar el XML de la factura (Ejemplo en Base64)
    attachment = etree.SubElement(root_ad, f"{{{CAC_NS}}}Attachment")
    external_reference = etree.SubElement(attachment, f"{{{CAC_NS}}}ExternalReference")
    etree.SubElement(external_reference, f"{{{CBC_NS}}}MimeCode").text = "text/xml"
    etree.SubElement(external_reference, f"{{{CBC_NS}}}EncodingCode").text = "UTF-8"
    description = etree.SubElement(external_reference, f"{{{CBC_NS}}}Description")
    description.text = etree.CDATA(xml_firm) #XML firmado
    

    #ParentDocumentLineReference
    parent_doc_line_ref = etree.SubElement(root_ad, f"{{{CAC_NS}}}ParentDocumentLineReference")
    etree.SubElement(parent_doc_line_ref, f"{{{CBC_NS}}}LineID").text = "1"
    doc_reference = etree.SubElement(parent_doc_line_ref, f"{{{CAC_NS}}}DocumentReference")
    etree.SubElement(doc_reference, f"{{{CBC_NS}}}ID").text = num_fac
    uuid_element = etree.SubElement(doc_reference, f"{{{CBC_NS}}}UUID", schemeID="1", schemeName=f"{attr_uuid}-SHA384")
    uuid_element.text = unique_code_hash
    etree.SubElement(doc_reference, f"{{{CBC_NS}}}IssueDate").text = issue_date
    etree.SubElement(doc_reference, f"{{{CBC_NS}}}DocumentType").text = "ApplicationResponse"
    attachment = etree.SubElement(doc_reference, f"{{{CAC_NS}}}Attachment")
    external_reference = etree.SubElement(attachment, f"{{{CAC_NS}}}ExternalReference")
    etree.SubElement(external_reference, f"{{{CBC_NS}}}MimeCode").text = "text/xml"
    etree.SubElement(external_reference, f"{{{CBC_NS}}}EncodingCode").text = "UTF-8"

    #Agregar <cbc:Description> con CDATA
    description = etree.SubElement(external_reference, f"{{{CBC_NS}}}Description")
    description.text = etree.CDATA(application_response) #Respuesta de la DIAN

    #Crear <cac:ResultOfVerification>
    verification = etree.SubElement(doc_reference, f"{{{CAC_NS}}}ResultOfVerification")
    etree.SubElement(verification, f"{{{CBC_NS}}}ValidatorID").text = "Unidad Especial Dirección de Impuestos y Aduanas Nacionales"
    etree.SubElement(verification, f"{{{CBC_NS}}}ValidationResultCode").text = validation_result_code #<cac:DocumentResponse><cac:Response><cbc:ResponseCode> de application response
    etree.SubElement(verification, f"{{{CBC_NS}}}ValidationDate").text = issue_date_ar #IssueDate de application response
    etree.SubElement(verification, f"{{{CBC_NS}}}ValidationTime").text = issue_time_ar #IssueTime de application response

    #return etree.tostring(root_ad, encoding="utf-8", xml_declaration=True, pretty_print=True).decode("utf-8")
    return etree.tostring(root_ad, encoding="utf-8", pretty_print=True).decode("utf-8")

def send_sqs(url_sqs:str,message:str,delay_seconds:int=0,attempts:int=0)->None:
    """
    Envía un mensaje a una cola SQS con delay y atributos personalizados.

    Args:
        url_sqs (str): URL de la cola SQS en AWS.
        message_body (dict): Cuerpo del mensaje a enviar.
        delay_seconds (int): Tiempo en segundos que SQS esperará antes de entregar el mensaje (0–900).
        attempts (int): Número de intentos realizados (se incluye como atributo del mensaje).
    """
    try:
        response = sqs.send_message(
            QueueUrl=url_sqs,
            MessageBody=message,
            DelaySeconds=delay_seconds,  # hasta 900 segundos (15 minutos)
            MessageAttributes={
                "attempts": {
                    "StringValue": str(attempts),
                    "DataType": "Number"
                }
            }
        )
        print("Mensaje enviado:", response["MessageId"])

    except Exception as e:
        print("Error al enviar a SQS:", e)

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

def update_traceability_process(table_name:str,num_fac:str,step:int,xmlSignatureStartDate:str,xmlSignatureDuration:str,dianConsuptionStartDate:str,dianConsuptionDuration:str)->None:
    table_traceability_process = dynamodb.Table(table_name)
    try:
        response = table_traceability_process.update_item(
            Key={
                'numFac': num_fac
            },
            UpdateExpression="SET step = :step, xmlSignatureStartDate = :v2, xmlSignatureDuration = :v3, dianConsuptionStartDate = :v4, dianConsuptionDuration = :v5",
            ExpressionAttributeValues={
                ':step': step,
                ':v2': xmlSignatureStartDate,
                ':v3': xmlSignatureDuration,
                ':v4': dianConsuptionStartDate,
                ':v5': dianConsuptionDuration
            },
            ReturnValues="UPDATED_NEW"
        )
    except Exception as e:
        print(e)

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


#Dividir todo el proceso en etapas
#Guardar en la BD el numero de etapa en la que va el proceso (o la etapa que finalizo)
#Deberia definir un parametro que si llega vacio inicia y si llega con etapa inicia en la etapa
#Crear lambda que me orqueste que tipo de etapa ejecutar para saber a donde envia, por ejemplo firmado XML, envio a la dian, crear PDF, envio email...
# - firmado del xml

CERTIFICATE = download_file(BUCKET_NAME_RESOURCES,CERT_PATH)
WSSE = download_file(BUCKET_NAME_RESOURCES,WSSE_PATH_DEV,True,"UTF-8")

def lambda_handler(event, context):
    environment = context.invoked_function_arn.split(":")[-1]
    HOST_DIAN = ""
    URL_QR_DIAN = ""
    response_service = {}
    code = -1
    message = ""
    validations = []
    error_message = ""
    response_message = ""
    qr_code = ""
    unique_code_hash = ""
    application_response = ""
    xml_firm = ""
    token = ""
    data = {} 

    '''
    #PARA PROBAR
    token = event.get("token")
    decoded_token = jwt.decode(
        token,
        SECRET_KEY,
        algorithms=["HS256"]  # Cambia el algoritmo según lo que uses
    )
    print(decoded_token)
    '''
        
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
        token = payload.get("token")
        tipo_envio = payload.get("tipoenvio")
        usuario = payload.get("usuario")
        sistema = payload.get("sistema")
        ip = payload.get("ip")
        factura = payload.get("factura")
        password_payload = payload.get("password")

        if not token:
            raise ValueError("No se proporcionó un token JWT.")

        # Validar el token JWT
        print("Validando token")
        print(token)
        #print(SECRET_KEY)
        #decoded_token = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        
        '''
        decoded_token = jwt.decode(
            token,
            SECRET_KEY,
            algorithms=["HS256"],  # Cambia el algoritmo según lo que uses
            options={"verify_exp": True}  # Verifica vigencia automáticamente
        )
        

        decoded_token = jwt.decode(
            token,
            SECRET_KEY,
            algorithms=["HS256"]  # Cambia el algoritmo según lo que uses
        )
        '''
        
        #Pruebas
        
        decoded_token = {
            'tin':'901188189',
            'user':'901188189',
            'password':'ba37949d3244ceff56ef73ddef3350fc54f29e21',
            'company':'fedelza'
        }        

        
        print(decoded_token)
        # Obtener el usuario del token
        tin = decoded_token.get("tin")
        user = decoded_token.get("user")

        table_user_name = f'{environment}_userApi'
        table_customer_dian_config_name = f'{environment}_customerDianConfig' 

        if not user:
            raise ValueError("El token no contiene un campo 'user'.")

        if exist_companyTin(table_user_name,tin):
            print(f"Nit {tin} encontrado en la tabla de usuarios del API")
            response_data_user = select_password(table_user_name,tin)
            print(f"Respuesta de la consulta de password: {response_data_user}")
            isActive = response_data_user['Item']['activeUser']
            print(f"Usuario activo: {isActive}")
            isBlocked = False  
            clave_tecnica = "dsknds7yfbdyzcys"
            software_pin = "12345"

            if (isActive):
                print("Usuario activo")
                if (isBlocked):
                    print("Usuario bloqueado")
                    raise ValueError("Usuario bloqueado")                   
                else:
                    print("Usuario correcto para validar")
                    #validar la contraseña enviada
                    storage_password = response_data_user['Item']['password']
                    password = decoded_token.get('password')
                    print(f"Contraseña enviada en el token: {password}")
                    print(f"Contraseña almacenada: {storage_password}")

                    if (storage_password == password):
                        # Generar token                                            
                        print("Inicio carga XML")
                        process_start_date = format_date(datetime.now(ZoneInfo("America/Bogota")))
                        xml_validation_start_time = time.time()
                        
                        ##REEMPLAZOS DE ERRORES SIESA##
                        #xml_bytes = xml_bytes.replace("CO, DIAN (Direccion de Impuestos y Aduanas Nacionales)", "CO, DIAN (Dirección de Impuestos y Aduanas Nacionales)")
                        factura = factura.replace("DIAN 2.1:DIAN 2.1:", "DIAN 2.1:")   
                        ##REEMPLAZOS DE ERRORES SIESA##

                        xml_bytes = factura.encode('utf-8')

                        root = etree.fromstring(xml_bytes)
                        print("XML cargado")
                        document_type = etree.QName(root).localname
                        print(f"Document type: {document_type}")
                        #Nota debito
                        #Solo el CUFE lleva clave tecnica
                        #Los demas llevan software pin

                        #tipos de documento:
                        #invoice
                        #creditNote
                        #debitNote...
                        #CUFE: Código Único de Factura Electrónica
                        #CUDE: Código Único de Documento Electrónico
                        #CUDS: Código Único de Documento Soporte 
                        #03 y 07 contingencia cliente
                        root_legal_monetary = ""
                        attr_uuid = ""
                        ar_abbreviation = "ar"
                        sufix_ds = ""
                        prefix = ""
                        document_type_abbreviation = ""
                        document_type_code = root.find(f"cbc:{document_type}TypeCode", namespaces=NAMESPACES).text
                        if document_type == "Invoice":
                            print("El nodo Invoice existe")
                            attr_uuid = 'CUFE' 
                            root_legal_monetary = "cac:LegalMonetaryTotal"
                            if document_type_code == '05':
                                document_type_abbreviation = 'ds'
                                ar_abbreviation = "ars"
                                attr_uuid = 'CUDS' 
                            else:
                                document_type_abbreviation = 'fv'
                            technical_key = LLAVE_TECNICA 
                            prefix = get_data(root,"invoice","authorization_invoices_prefix")           
                        elif document_type == "DebitNote":
                            print("El nodo DebitNote no existe")
                            attr_uuid = 'CUDE' 
                            root_legal_monetary = "cac:RequestedMonetaryTotal"
                            if document_type_code == '93':
                                document_type_abbreviation = 'ncs' 
                            else:
                                document_type_abbreviation = 'nd' 
                            technical_key = SOFTWARE_PIN
                            prefix = "ND"
                        elif document_type == "CreditNote":
                            print("El nodo CreditNote no existe")                        
                            attr_uuid = 'CUDE' 
                            root_legal_monetary = "cac:LegalMonetaryTotal"
                            if document_type_code == '95':
                                document_type_abbreviation = 'nas'
                                attr_uuid = 'CUDS'
                            else:
                                document_type_abbreviation = 'nc'
                            technical_key = SOFTWARE_PIN
                            prefix = "NC"      
                        else:
                            print("El xml no tiene una estructura correcta")
                        

                        #Capturar la data principal
                        num_fac_element = root.find(XML_PATHS['invoice']['id'], namespaces=NAMESPACES)   
                        num_fac = get_data(root,"invoice","id")
                        
                        print(f"num_fac: {num_fac}")                                
                    
                        
                        customer_id_element = root.find(XML_PATHS['customer']['id'], namespaces=NAMESPACES)
                        customer_id = customer_id_element.text
                        print(f"customer: {customer_id}")
                        supplier_id_element = root.find(XML_PATHS['supplier']['id'], namespaces=NAMESPACES)
                        supplier_id = supplier_id_element.text
                        print(f"supplier: {supplier_id}")
                        customer_registration_name = get_data(root,"customer","registration_name")
                        issue_time = get_data(root,"invoice","issue_time") 
                        profile_execution_id = get_data(root,"invoice","profile_execution_id")
                        supplier_registration_name = get_data(root,"supplier","registration_name")
                        supplier_scheme_name = supplier_id_element.get("schemeName")
                        supplier_scheme_id = supplier_id_element.get("schemeID")
                        customer_scheme_name = customer_id_element.get("schemeName")
                        customer_scheme_id = customer_id_element.get("schemeID")

                        #Capturar Información del digito de verificación
                        software_provider_scheme_id = supplier_id_element.attrib['schemeID']
                        
                        #Validacion del tipo de documento para documento soporte
                        if document_type_abbreviation == "ds" or document_type_abbreviation == "nas":
                            print("###########DOCUMENTO SOPORTE###########")
                            sufix_ds = "DS"
                            Prefix = "DS"
                            supplier_id_ant = supplier_id
                            supplier_id = customer_id
                            customer_id = supplier_id_ant
                            software_provider_scheme_id = customer_id_element.attrib['schemeID']
                        
                        issue_date_element = root.find('cbc:IssueDate', namespaces=NAMESPACES)
                        issue_date = issue_date_element.text
                        
                        
                        #currency = get_data(root, "invoice", "currency")
                        currency = ""
                        pattern_currency = r"<MonedaCop>(.*?)</MonedaCop>"
                        
                        match_currency = re.search(pattern_currency, factura)

                        if match_currency:
                            currency = match_currency.group(1)
                        else:
                            print("❌ No se encontró MonedaCop")
                        
                        #trm = get_data(root, "invoice", "trm")
                        trm = ""
                        pattern_trm = r"<FctConvCop>(.*?)</FctConvCop>"
                        
                        match_trm = re.search(pattern_trm, factura)

                        if match_trm:
                            trm = match_trm.group(1)
                        else:
                            print("❌ No se encontró FctConvCop")
                        
                        

                        bucket_name_company = f"fe.{environment.lower()}.{supplier_id}"
                        print(f"Nombre del bucket: {bucket_name_company}")

                        if (environment == "Prod"):
                            print("################# AMBIENTE PROD #################")
                            schemeIDAmbiente = "1"
                            HOST_DIAN = HOST_DIAN_PROD   
                            URL_QR_DIAN = URL_QR_DIAN_PROD                         
                        else:
                            print("################# AMBIENTE DEV #################")
                            schemeIDAmbiente = "2"
                            HOST_DIAN = HOST_DIAN_DEV
                            URL_QR_DIAN = URL_QR_DIAN_DEV


                        #TODO
                        #Debo configurar el timeout y realizar los reintentos desde una cola
                        #Para que despues cuando me envien la consulta del estado tenerla ya en BD
                        #Revisar numeracion enviada en las tablas de configuracion del cliente
                    
                        if environment == "Prod":
                            validating = validate_numbering(table_customer_dian_config_name,supplier_id,num_fac)
                            if validating['valid']:
                                print("Numeración válida")
    
                            else:                            
                                message = f"La numeración enviada no corresponde con la numeracion configurada para el nit {supplier_id}"
                                print(message)
                                raise ValueError(message)                            
                        

                        #Revisar que la fecha enviada sea igual a la fecha actual
                        #Revisar aca si la factura ya fue procesada anteriormente
                        table_name = f"{environment}_{supplier_id}_dianStatus"
                        dian_audit_table_name = f"{environment}_{supplier_id}_dianAudit"
                        dian_status_table_name = f"{environment}_{supplier_id}_dianStatus"
                        traceability_process_table_name = f"{environment}_{supplier_id}_traceabilityProcess"
                        invoice_response = get_invoice(table_name,num_fac)
                        print("Finaliza consulta de la factura")
                        if invoice_response is None:
                            #Hacer prueba de timeout
                            #1. Configurar timeout menor en el API gateway
                            #2. Verificar que la lambda finaliza
                            #3. Ver que estado se devuelve a postman
                            print("Insertar proceso a trazabilidad para control")
                            insert_traceability_process(traceability_process_table_name,num_fac,process_start_date)

                            #Consultar estado de la DIAN
                            service_status_table_name = f"{environment}_controlStatus"
                            service_available = is_dian_available(service_status_table_name)
                            if service_available:
                                print("DIAN disponible")
                            else:
                                message = "Servicio de la DIAN no disponible"
                                print(message)
                                raise ValueError(message)


                            #Si issue date es menor a la fecha actual la cambio por la fecha del dia para que no genere error en la DIAN
                            #AAAA-MM-DD
                            actual_issue_date = f"{AAAA}-{MM}-{DD}"
                            issue_date_dt = datetime.strptime(issue_date, "%Y-%m-%d").date()
                            actual_date_dt = datetime.strptime(actual_issue_date, "%Y-%m-%d").date()

                            # Comparar fechas
                            if issue_date_dt < actual_date_dt:
                                print("Se realiza cambio de la fecha issue date")
                                print(f"IssueDate en XML: {issue_date}")
                                issue_date_element.text = actual_issue_date
                                issue_date = actual_issue_date
                                                        
                            val_fac_xpath = f'{root_legal_monetary}/cbc:LineExtensionAmount'                        
                            val_tot_fac_xpath = f'{root_legal_monetary}/cbc:PayableAmount'                        
                            val_fac = root.find(f'{val_fac_xpath}', namespaces=NAMESPACES).text                        
                            val_tot_fac = root.find(f'{val_tot_fac_xpath}', namespaces=NAMESPACES).text                                
                                                                                                                                                                                            
                            #Capturar la data secundaria
                            company = decoded_token.get("company")                            
                                                                                                                                                    
                                                                                                            
                            customer_name = get_data(root,"customer","name")
                            customer_email = get_data(root,"customer","email")
                            software_security_code_id = generate_sha384_hash(clave_tecnica, software_pin, num_fac)
                            nnnnnnnnnn = supplier_id.zfill(10) if len(supplier_id) <= 10 else supplier_id
                            dddddddd = num_fac.zfill(8) if len(num_fac) <= 8 else num_fac

                            path_files = f"{AAAA}/{MM}/{DD}/{document_type}/{num_fac}"
                            file_name = f'{nnnnnnnnnn}{PPP}{AA}{dddddddd}'  
                            subject = f"{supplier_id};{supplier_registration_name};{num_fac};{document_type_code};{supplier_registration_name};"                      
                            #Guardar archivo XML original
                            print("Inicio de carga de XML Original a S3")  
                            save_document(bucket_name_company,path_files,"xml",factura,file_name,document_type_abbreviation) #factura de venta, nota credito o nota debito

                            
                            #Creacion CUFE y QR
                            ########################################     REVISAR VALORES DE TAXVALUES    ###########################################
                            tax_values = {'01': 0.0, '03': 0.0, '04': 0.0}
                            val_oth_imp = 0.0
                            tax_totals = root.findall('cac:TaxTotal', namespaces=NAMESPACES)
                            tax_Array = []
                            tax_dict = {}
                            global_tax_amount = ""
                            global_tax_impoconsumo = ""
                            global_tax_bag = ""
                            for tax_total in tax_totals:
                                tax_subtotals = tax_total.xpath('.//cac:TaxSubtotal', namespaces=NAMESPACES)

                                for tax_subtotal in tax_subtotals:
                                    # Extraer el código de impuesto (01:IVA, IC:02, INC)
                                    codigo_nodes = tax_subtotal.xpath('.//cac:TaxCategory/cac:TaxScheme/cbc:ID', namespaces=NAMESPACES)
                                    if not codigo_nodes or codigo_nodes[0].text is None:
                                        continue  # Si no se encuentra el código, se salta este subtotal
                                    codigo_imp = codigo_nodes[0].text.strip()

                                    # Extraer el valor del impuesto. Se asume que el valor está en el nodo cbc:TaxAmount
                                    tax_amount_nodes = tax_subtotal.xpath('.//cbc:TaxAmount', namespaces=NAMESPACES)
                                    if not tax_amount_nodes or tax_amount_nodes[0].text is None:
                                        continue

                                    # Obtener el texto y reemplazar la coma por punto
                                    tax_amount_text = tax_amount_nodes[0].text.strip()
                                    try:
                                        tax_amount = float(tax_amount_text.replace(',', '.'))
                                    except Exception as e:
                                        print(f"Error al convertir el monto '{tax_amount_text}' a número: {e}")
                                        continue
                                    tax_amount_node_1 = tax_total.xpath('.//cbc:TaxAmount', namespaces=NAMESPACES)
                                    tax_amount_text_1 = tax_amount_node_1[0].text.strip()
                                    # Obtener datos de IVA y agregarlos a un array
                                    if codigo_imp == '01':
                                        
                                        global_tax_amount = tax_amount_text_1
                                        tax_percentage = tax_subtotal.xpath('.//cbc:Percent', namespaces=NAMESPACES)[0].text.strip()
                                        if tax_percentage != "":
                                            tax_percentage += "%" 
                                        tax_dict = {
                                            'tax_category': codigo_imp,
                                            'tax_percentage': tax_percentage,
                                            'tax_amount': tax_amount,
                                            'taxable_amount': tax_subtotal.xpath('.//cbc:TaxableAmount ', namespaces=NAMESPACES)[0].text.strip()
                                        }
                                        tax_Array.append(tax_dict)
                                    elif codigo_imp == "02":
                                        global_tax_impoconsumo = tax_amount_text_1
                                    elif codigo_imp == "22":
                                        global_tax_bag = tax_amount_text_1

                                    # Si el código de impuesto no es '01', se acumula en el valor de otros impuestos
                                    if codigo_imp != '01':
                                        val_oth_imp += tax_amount

                                    # Acumular el monto en el diccionario
                                    if codigo_imp in tax_values:
                                        tax_values[codigo_imp] += tax_amount
                                    else:
                                        tax_values[codigo_imp] = tax_amount

                            # Formatear los valores a dos decimales
                            for key in tax_values:
                                tax_values[key] = "{:.2f}".format(tax_values[key])
                                
                            val_oth_imp = "{:.2f}".format(val_oth_imp)
                            
                            #XML_PATHS['customer']['email']
                            #"taxable_amount": "cac:TaxTotal/cac:TaxSubtotal/cbc:TaxableAmount",  # Base gravable
                            #"tax_amount": "cac:TaxTotal/cbc:TaxAmount",  # Valor del impuesto
                            #"tax_category": "cac:TaxTotal/cac:TaxSubtotal/cac:TaxCategory/cac:TaxScheme/cbc:Name",  # Tipo de impuesto (ej. IVA)
                            #"tax_percentage": "cac:TaxTotal/cac:TaxSubtotal/cbc:Percent",  # Porcentaje del impuesto
                            #print(num_fac,issue_date,issue_time,val_fac,'01',tax_values['01'],'04',tax_values['04'],'03',tax_values['03'],val_tot_fac,supplier_id,customer_id,technical_key, profile_execution_id)
                            unique_code_hash = generate_sha384_hash(num_fac,issue_date,issue_time,val_fac,'01',tax_values['01'],'04',tax_values['04'],'03',tax_values['03'],val_tot_fac,supplier_id,customer_id,technical_key, profile_execution_id)
                            qr_code = f"NumFac:{num_fac} FecFac:{issue_date} HorFac:{issue_time} NitFac:{supplier_id} DocAdq:{customer_id} ValFac:{val_fac} ValIva:01 ValOtroIm:{val_oth_imp} ValTolFac:{val_tot_fac} {attr_uuid}:{unique_code_hash} QRCode:{URL_QR_DIAN}{unique_code_hash}"
                            

                            ublextensions = root.findall('ext:UBLExtensions', namespaces=NAMESPACES)
                            ublextension = ublextensions[0].find('ext:UBLExtension', namespaces=NAMESPACES)
                            extension_content = ublextension.find('ext:ExtensionContent', namespaces=NAMESPACES)
                            dian_extensions = extension_content.find('sts:DianExtensions', namespaces=NAMESPACES)
                                                                                                    
                            print("Asignacion UUID")
                            #Asignacion UUID
                            uuid_element = etree.SubElement(root, '{urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2}UUID')
                            uuid_element.set('schemeID', schemeIDAmbiente)
                            uuid_element.set('schemeName', f"{attr_uuid}-SHA384")
                            uuid_element.text = unique_code_hash
                            num_fac_element.addnext(uuid_element)

                                                        
                            print("Asignacion software provider")
                            #Asignacion SoftwareProvider Solo para las facturas
                            #Habilitar para todos los tipos de documento
                            if True:
                            #if document_type == "Invoice":                      
                                # Buscar nodo <SoftwareProvider>
                                software_provider_element = dian_extensions.find('sts:SoftwareProvider', namespaces=NAMESPACES)
                                if software_provider_element is None:
                                    software_provider_element = etree.SubElement(dian_extensions, '{dian:gov:co:facturaelectronica:Structures-2-1}SoftwareProvider')
                                else:
                                    # Si existe, limpiar el contenido previo
                                    software_provider_element.clear()
                                                                
                                #ProviderId
                                provider_id_element = etree.SubElement(software_provider_element, '{dian:gov:co:facturaelectronica:Structures-2-1}ProviderID')
                                provider_id_element.set("schemeAgencyID","195")
                                provider_id_element.set("schemeAgencyName",SCHEME_AGENCY_NAME)
                                provider_id_element.set("schemeID",software_provider_scheme_id)
                                provider_id_element.set("schemeName",SOFTWARE_PROVIDER_SCHEME_NAME)
                                provider_id_element.text = supplier_id #SOFTWARE_PROVIDER_TIN (Se debe poner aca el nit del cliente Supplier)
                                #SoftwareID
                                print("Asignacion software id")
                                software_id_element = etree.SubElement(software_provider_element, '{dian:gov:co:facturaelectronica:Structures-2-1}SoftwareID')
                                software_id_element.set("schemeAgencyID","195")
                                software_id_element.set("schemeAgencyName",SCHEME_AGENCY_NAME)
                                software_id_element.text = SOFTWARE_ID  

                                # Buscar nodo <SoftwareSecurityCode>
                                software_security_code_element = dian_extensions.find('sts:SoftwareSecurityCode', namespaces=NAMESPACES)
                                if software_security_code_element is None:
                                    software_security_code_element = etree.SubElement(dian_extensions, '{dian:gov:co:facturaelectronica:Structures-2-1}SoftwareSecurityCode')
                                else:
                                    software_security_code_element.clear()

                                print("Asignacion security code")
                                #Asignacion SoftwareSecurityCode                                
                                software_security_code_element.set("schemeAgencyID","195")
                                software_security_code_element.set("schemeAgencyName",SCHEME_AGENCY_NAME)
                                security_code_hash = generate_sha384_hash(SOFTWARE_ID,SOFTWARE_PIN,num_fac)
                                software_security_code_element.text = security_code_hash
                                #dian_extensions.append(software_security_code_element)                                                


                            #Asignacion AuthorizationProvider
                            '''
                            authorization_provider_element = etree.SubElement(dian_extensions,'{dian:gov:co:facturaelectronica:Structures-2-1}AuthorizationProvider')
                            #AuthorizationProviderID
                            authorization_provider_id_element = etree.SubElement(authorization_provider_element, '{dian:gov:co:facturaelectronica:Structures-2-1}AuthorizationProviderID')
                            authorization_provider_id_element.set("schemeAgencyID","195")
                            authorization_provider_id_element.set("schemeAgencyName",SCHEME_AGENCY_NAME)
                            authorization_provider_id_element.set("schemeID","4")
                            authorization_provider_id_element.set("schemeName","31")
                            authorization_provider_id_element.text = '800197268'                         
                            dian_extensions.append(authorization_provider_id_element)
                            '''

                            print("Asignacion QR")
                            #Asignacion QR  (Si el nodo QRCode no existe)                       
                            '''
                            qr_code_element = etree.Element('{dian:gov:co:facturaelectronica:Structures-2-1}QRCode')
                            qr_code_element.text = qr_code
                            dian_extensions.append(qr_code_element)
                            '''
                            #Asignacion QR  (Si el nodo QRCode ya existe)
                            qr_element = dian_extensions.find('sts:QRCode', namespaces=NAMESPACES)
                            qr_element.text = qr_code
                            
                            
                            data["numFac"] = num_fac
                            data["QR"] = qr_code
                            data["documentTypeAbbreviation"] = document_type_abbreviation
                            data["attrUuid"] = attr_uuid
                            data["currency"] = currency
                            data["trm"] = trm
                            data["CUFE-CUDE"] = unique_code_hash
                            data["valTotFac"] = val_tot_fac
                            data["fileName"] = file_name
                            data["filePath"] = path_files
                            data["issueDate"] = issue_date
                            data["issueTime"] = issue_time
                            data["subject"] = subject
                            data["dueDate"] = get_data(root,"invoice","due_date")
                            data["invoiceAuthorization"] = get_data(root,"invoice","invoice_authorization")
                            data["authorizationStartPeriod"] = get_data(root,"invoice","authorization_start_period")
                            data["authorizationEndPeriod"] = get_data(root,"invoice","authorization_end_period")
                            data["authorizationInvoicesPrefix"] = prefix
                            data["authorizationInvoicesFrom"] = get_data(root,"invoice","authorization_invoices_from")
                            data["authorizationInvoicesTo"] = get_data(root,"invoice","authorization_invoices_to")
                            
                            data["invoiceReferenceId"] = get_data(root,"invoice","invoiceReferenceId")
                            data["invoiceReferenceDate"] = get_data(root,"invoice","invoiceReferenceDate")
                            data["invoiceUuid"] = get_data(root,"invoice","invoiceUuid")
                            
                            payment_id = get_data(root,"invoice","payment_id")
                            if payment_id == '1':
                                data["paymentId"] = 'Contado'
                            elif payment_id == '2':
                                data["paymentId"] = 'Crédito'
                            
                            '''
                            elif payment_id == 'ZZZ':
                                data["paymentId"] = 'Acuerdo mutuo'
                            '''
                            
                            print("Inicio busqueda concept")
                            concept = get_data(root, "invoice", "concept")
                            data["concept"] = concept

                            #Se valida que el concepto este vacio porque cuando se agrega la nota 1
                            #se esta agregando una nota de mas y hace que se corran los campos

                            if (concept == ''):
                                data["totalInWords"] = get_data(root,"invoice","totalInWords1")
                                data["term"] = get_data(root,"invoice","term1") #plazo
                                data["seller"] = get_data(root,"invoice","seller1") #vendedor

                            else:
                                data["totalInWords"] = get_data(root,"invoice","totalInWords2")
                                data["term"] = get_data(root,"invoice","term2") #plazo
                                data["seller"] = get_data(root,"invoice","seller2") #vendedor

                            allowanceTotalAmount_s = get_data(root,"legal_monetary_total", "allowance_total_amount")    # descuento
                            taxExclusiveAmount_s = get_data(root,"legal_monetary_total", "tax_exclusive_amount")    # Total bruto
                            if (allowanceTotalAmount_s == ''):
                                allowanceTotalAmount = ""
                            else:    
                                allowanceTotalAmount = Decimal(allowanceTotalAmount_s)    # descuento
                            if (taxExclusiveAmount_s == ''):
                                taxExclusiveAmount = ""
                            else:
                                taxExclusiveAmount = Decimal(taxExclusiveAmount_s)    # Total bruto

                            print("ACA VOY")    
                            allowanceTotalAmount = str(allowanceTotalAmount.quantize(Decimal("0.00"), rounding=ROUND_HALF_UP))
                            data["allowanceTotalAmount"] = allowanceTotalAmount
                            taxExclusiveAmount = str(taxExclusiveAmount.quantize(Decimal("0.00"), rounding=ROUND_HALF_UP))
                            data["taxExclusiveAmount"] = taxExclusiveAmount

                            data["supplierRegistrationName"] = supplier_registration_name
                            data["supplierId"] = supplier_id
                            data["supplierIndustryClassificationCode"] = get_data(root,"supplier","industry_classification_code")
                            data["supplierAddressLine"] = get_data(root,"supplier","address")
                            data["supplierCityName"] = get_data(root,"supplier","city")
                            data["supplierCountrySubentity"] = get_data(root,"supplier","country")
                            data["supplierTelephone"] = get_data(root,"supplier","telephone")
                            data["supplierElectronicMail"] = get_data(root,"supplier","email")
                            
                            data["customerRegistrationName"] = customer_registration_name
                            data["customerName"] = customer_name
                            data["customerId"] = customer_id
                            data["customerAddressLine"] = get_data(root,"customer","address")
                            data["customerCityName"] = get_data(root,"customer","city")
                            data["customerCountrySubentity"] = get_data(root,"customer","country")
                            data["customerTelephone"] = get_data(root,"customer","telephone")
                            data["customerElectronicMail"] = customer_email
                            data["customerNeighborhood"] = get_data(root,"customer","neighborhood") #barrio    
                            print("Inicio busqueda payment means")

                            #PENDIENTES NOTAS
                            data["authorized"] = get_data(root, "invoice", "authorized")
                            data["noteType"] = get_data(root, "invoice", "noteType")

                            payment_mean_code = get_data(root,"invoice","payment_mean_code") #medio pago
                            if payment_mean_code == '10':
                                data["paymentMeansCode"] = 'Efectivo'
                            elif payment_mean_code == '20':
                                data["paymentMeansCode"] = 'Cheque'
                            elif payment_mean_code == '48':
                                data["paymentMeansCode"] = 'T. Crédito'
                            elif payment_mean_code == '49':
                                data["paymentMeansCode"] = 'T. Débito'
                            elif payment_mean_code == '42':
                                data["paymentMeansCode"] = 'C. Bancaria'
                            elif payment_mean_code == 'ZZZ':
                                data["paymentMeansCode"] = 'Acuerdo Mutuo'
                            else:
                                data["paymentMeansCode"] = 'Otro'

                            print("Inicio busqueda payment due date")    
                            data["expires"] = get_data(root,"payment","payment_due_date") #vence                        
                            data["purchaceOrder"] = get_data(root,"invoice","purchace_order") #orden compra
                            data["orderReference"] = get_data(root, "invoice", "order_reference") #referencia orden
                            print("Inicio busqueda payment reference")
                            # --- Campos de totales generales (LegalMonetaryTotal) ---
                            data["lineExtensionAmount"] = get_data(root,"legal_monetary_total", "line_extension_amount")  # total bruto
                            
                            data["advanceAmount"] = get_data(root,"legal_monetary_total", "advance_amount")            # anticipo
                            
                            
                            data["payableAmount"] = get_data(root,"legal_monetary_total", "payable_amount")          # total
                            # --- Campos de impuestos (TaxTotal) ---
                            data["taxAmount"] = global_tax_amount                              # iva
                            data["impoconsumoAmount"] = global_tax_impoconsumo               # impoconsumo
                            data["bagTaxAmount"] = global_tax_bag                  # impuesto bolsa
                            data["taxScheme"] = get_data(root,"taxTotal","tax_scheme") #id impuesto
                            data["taxableAmount"] = get_data(root,"taxTotal","taxable_amount") #impoconsumo
                            data["impTaxAmount"] = get_data(root,"taxTotal","imp_tax_amount") #impuesto bolsa

                            # --- (Opcional) Campos de desglose de impuestos (TaxBreakdown) ---
                            data["taxIva"] = get_data(root,"tax_breakdown", "iva")                               # iva (segundo bloque)
                            data["taxImpoconsumo"] = get_data(root,"tax_breakdown", "impoconsumo")                # impoconsumo (segundo bloque)
                            data["taxImpuestoBolsa"] = get_data(root,"tax_breakdown", "bag_tax")            # impuesto bolsa (segundo bloque)
                            data["taxObsequio"] = get_data(root,"tax_breakdown", "gift")     # obsequio (segundo bloque, opcional)
                            data["taxAnticipo"] = get_data(root,"tax_breakdown", "advance")     # anticipo (segundo bloque, opcional)
                            data["taxTotal"] = get_data(root,"tax_breakdown", "total")                           # total (segundo bloque)
                                                                                            
                            
                            #Informacion para la creacion del PDF                    
                            lines = root.findall(f"cac:{document_type}Line", namespaces=NAMESPACES)
                                                
                            lines_Array = []
                            line_dict = {}
                            items = 0
                            result_gift = Decimal("0.00")

                            for line in lines:
                                #Validacion de impuestos a nivel de items
                                tax_percentage = "" 
                                consumption_tax = Decimal(0)
                                tax_totals = line.findall(XML_PATHS['items']['tax_total'], namespaces=NAMESPACES) 
                                for tax_total in tax_totals:
                                    tax_id = tax_total.find(XML_PATHS['items']['taxTotal']['tax_id'], namespaces=NAMESPACES).text
                                    if tax_id == "01":
                                        tax_percentage_element = tax_total.find(XML_PATHS['items']['taxTotal']['tax_percentage'], namespaces=NAMESPACES)
                                        if tax_percentage_element is not None:
                                            tax_percentage = tax_percentage_element.text
                                        else:
                                            tax_percentage = ""
                                    elif tax_id == "02" or tax_id == "04" or tax_id == "22" or tax_id == "23" or tax_id == "34" or tax_id == "35":
                                        consumption_tax = Decimal(tax_total.find(XML_PATHS['items']['taxTotal']['impoconsumo_amount'], namespaces=NAMESPACES).text)
                                                                    
                                
                                #Realizar sumatoria
                                quantity = Decimal(get_data(line, "items", "quantity", default="0"))
                                unit_price =  Decimal(get_data(line, "items", "unit_price",default="0"))
                                discount =  Decimal(get_data(line, "items", "discount_amount",default="0"))
                                #Es (vr unitario x Cantidad - Descuentos )+ Impuestos
                                result = (unit_price * quantity - discount) + consumption_tax
                                amount = str(result.quantize(Decimal("0.00"), rounding=ROUND_HALF_UP))
                                #amount real
                                #amount = get_data(line, "items", "line_extension_amount")  
                                
                                
                                #Realizar sumatoria de los obsequios
                                free_charge_indicator = get_data(line, "items", "free_charge_indicator")
                                if free_charge_indicator == "true":
                                    amount = "Obsequio"
                                    result_gift += (unit_price * quantity)                                                                
                                                                                        
                                if consumption_tax == 0:
                                    consumption_tax = ""
                                else:
                                    consumption_tax = str(consumption_tax)

                                discount_percentage = get_data(line, "items", "discount_percentage")
                                if discount_percentage != "":
                                    discount_percentage += "%"
                                
                                description = get_data(line, "items", "description")
                                lote = get_data(line, "items", "lote")
                                if lote != "":
                                    description += "\n" + lote

                                if tax_percentage != "":
                                    tax_percentage += "%"
                                if document_type_abbreviation == "fv":
                                    line_dict = {  # Se crea un nuevo diccionario en cada iteración
                                        "id": get_data(line, "items", "id"),
                                        "description": description,
                                        "unitOfMeasure": get_data(line, "items", "unit_of_measure"),
                                        "quantity": get_data(line, "items", "quantity"),
                                        "unitPrice": get_data(line, "items", "unit_price"),
                                        "discountPercentage": discount_percentage,
                                        "vatPercentage": tax_percentage,
                                        "consumptionTax": consumption_tax,
                                        "amount": amount,
                                    }
                                else:
                                    line_dict = {  # Se crea un nuevo diccionario en cada iteración
                                        "id": get_data(line, "items", "id"),
                                        "description": description,
                                        "mark": "",
                                        "unitOfMeasure": get_data(line, "items", "unit_of_measure"),
                                        "quantity": get_data(line, "items", "quantity"),
                                        "unitPrice": get_data(line, "items", "unit_price"),
                                        "discountPercentage": discount_percentage,
                                        "vatPercentage": tax_percentage,
                                        "consumptionTax": consumption_tax,
                                        "amount": amount,
                                    }

                                lines_Array.append(line_dict)
                                items += 1                                    
                            
                            data["items"] = items
                            data["lines"] = lines_Array
                            data["tax"] = tax_Array 
                            
                            print("Validacion gift")
                            if result_gift == 0.0:
                                gift_amount = ""
                            else:
                                gift_amount = str(result_gift.quantize(Decimal("0.00"), rounding=ROUND_HALF_UP))                      
                            data["giftAmount"] = gift_amount
                            message = json.dumps(data)
                            save_document(bucket_name_company,path_files,"json",message,file_name,"") #json de generacion PDF
                            print("Termina asignación de data para el PDF")
                            #Termina asignacion de data


                            #Realizar el firmado del XML
                            print("Inicio de firmado del XML") 
                            xml_signature_start_date = format_date(datetime.now(ZoneInfo("America/Bogota")))
                            xml_signature_start_time = time.time()
                            c14n_xml = etree.tostring(root, method="c14n", exclusive=False, with_comments=False)
                            save_document(bucket_name_company,path_files,"xml",c14n_xml,file_name,"f2f") #factura firmada
                            factura_bytes = etree.tostring(root, encoding='utf-8')
                            password_bytes = CERT_PASSWORD.encode('utf-8')
                            xml_firm = firm_document(c14n_xml, CERTIFICATE, password_bytes, "third party", now)
                            xml_signature_end_time = time.time()
                            xml_signature_execution_time_ms = int((xml_signature_end_time - xml_signature_start_time) * 1000)
                            
                            
                            #Realizar la carga del XML Firmado a S3
                            print("Inicio de carga de XML Firmado a S3")                        
                            save_document(bucket_name_company,path_files,"xml",xml_firm,file_name,"ff") #factura firmada
                            #Crear el comprimido del XML y convertir a Base64
                            content_b64 = create_zip(xml_firm,file_name)                        
                            #Realizar la carga del zip a S3
                            print("Inicio de carga del ZIP a S3")                        
                            save_document(bucket_name_company,path_files,"b64",content_b64,file_name,"b64") #Base64 con el zip


                            #Realizar el consumo de la dian
                            dian_start_date = format_date(datetime.now(ZoneInfo("America/Bogota")))
                            dian_start_time = time.time()
                            #Crear request para la DIAN
                            #Proceso de habilitacion
                            '''
                            soap_parameters = {
                                "fileName": f"z{file_name}.zip",
                                "contentFile": content_b64,
                                "testSetId": TEST_ID
                            }
                            method = "SendTestSetAsync"
                            soap_payload = create_soap_payload(method,WSSE,URL_DIAN,soap_parameters)

                            #Ejemplo
                            #environment_type = root.find(XML_PATHS['invoice']['profile_execution_id'], namespaces=NAMESPACES).text
                            zip_key = root_response.find(f"s:Body/wcf:{method}Response/wcf:{method}Result/b:ZipKey", namespaces=namespaces_response)
                            zip_key_value = zip_key.text if zip_key is not None else "No encontrado"
                            print("ZipKey:", zip_key_value)

                            #Para validar despues
                            #if type(response_data) == dict and "statusCode" in response_data and response_data['statusCode'] == 504:
                                #return response_data,''

                            #zip_key_value = '5189f609-cc9e-46bc-9ce0-717da4f2a565'
                            #zip_key_value = '84b49d3a-54e9-4cbc-a0b7-063e326cbdd3'

                            #Crear request para consultar estado en la DIAN
                            soap_parameters2 = {
                                "trackId": zip_key_value
                            }
                            method = "GetStatusZip"
                            soap_payload = create_soap_payload(method,WSSE,URL_DIAN,soap_parameters2)
                            print(soap_payload)
                            time.sleep(1)
                            response_data2 = get_response_dian(HOST_DIAN, soap_payload)
                            print("Respuesta DIAN2:")
                            print(response_data2)

                            # Parsear el XML                        
                            root_response2 = etree.fromstring(response_data2)
                            # Buscar el elemento StatusDescription
                            status_desc_elem = root_response2.find(f's:Body/{method}Response/{method}Result/b:DianResponse/b:StatusDescription', namespaces=namespaces_response)
                            print(status_desc_elem)
                            if status_desc_elem is not None and status_desc_elem.text:
                                print("If")
                                status_text = status_desc_elem.text.strip()
                                print("StatusDescription:", status_text)
                                
                                # Validar si contiene el texto "Batch en proceso"
                                if "Batch en proceso" in status_text:
                                    print("El status description contiene 'Batch en proceso'.")
                                    time.sleep(1000)
                                    response_data2 = get_response_dian(HOST_DIAN, soap_payload)
                                    print("Respuesta DIAN3:")
                                    print(response_data2)
                                else:
                                    print("El status description NO contiene 'Batch en proceso'.")
                            else:
                                print("No se encontró el elemento StatusDescription en el response.")
            
                            
                            '''
                            #Proceso sincrono facturacion
                            soap_parameters = {
                                "fileName": f"z{file_name}.zip",
                                "contentFile": content_b64
                            }
                            method = "SendBillSync"
                            soap_payload = create_soap_payload(method,WSSE,URL_DIAN,soap_parameters)                        
                            #Realizar la carga del XML request a S3
                            print("Inicio de carga de XML request a S3")                        
                            save_document(bucket_name_company,path_files,"xml",soap_payload,file_name,"req") #Request
                            #realizar el envio del XML a la DIAN
                            print("Inicio de envio del XML a la DIAN")
                            response_data = get_response_dian(HOST_DIAN, soap_payload)
                            if response_data is None:
                                #La dian esta generando timeout   
                                dian_status = "Timeout"
                                process_dian_start_date = format_date(datetime.now(ZoneInfo("America/Bogota")))
                                insert_dian_audit(dian_audit_table_name,num_fac,process_start_date,customer_registration_name,int(customer_id),issue_date,dian_status)
                                insert_dian_status(dian_status_table_name,num_fac,process_dian_start_date,customer_registration_name,int(customer_id),unique_code_hash,qr_code,issue_date,dian_status,document_type,document_type_abbreviation,path_files,file_name,prefix,val_tot_fac,error_message,"")
                                #Enviar mensaje a la cola con retardo de X segundos
                                send_sqs(f"{URL_SQS}{environment}_dian_retryTimeOut",payload,10) #Delay de 10 segundos
                                message = "TimeOut en la DIAN"
                                raise ValueError(message)
                                
                                            
                            dian_end_time = time.time()
                            dian_execution_time_ms = int((dian_end_time - dian_start_time) * 1000)
                            print("Tiempo de ejecución de la DIAN:", dian_execution_time_ms, "ms")
                            #Inserto step 2: Enviando a la DIAN
                            update_traceability_process(traceability_process_table_name,num_fac,2,xml_signature_start_date,xml_signature_execution_time_ms,dian_start_date,dian_execution_time_ms)
                            #Realizar la carga del XML response a S3
                            print("Inicio de carga de XML response a S3")                        
                            save_document(bucket_name_company,path_files,"xml",response_data,file_name,"res") #Response
                            #Consultar respuesta de habilitacion
                            root_response = etree.fromstring(response_data)                        
                                                    
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
                                    save_document(bucket_name_company,path_files,"xml",application_response,file_name,ar_abbreviation)
                                    print("OK")
                                except Exception as e:                            
                                    print(f"Error al decodificar el contenido Base64: {e}")
                                    raise ValueError(f"Error al decodificar el contenido Base64: {e}")
                                                            
                                if status_code == "00":
                                    print("StatusCode es igual a 0, la factura fue aprobada")
                                    dian_status = "Aprobado"
                                    #Enviar a crear el PDF
                                    print("Enviando datos para crear PDF")
                                    message = json.dumps(data)
                                    print(response_data)
                                    send_sqs(f"{URL_SQS}{environment}_Pdf_Create_Ondemand",message)
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

                            #Insertar en dian process el estado de la validacion ante la DIAN
                            process_dian_start_date = format_date(datetime.now(ZoneInfo("America/Bogota")))
                            insert_dian_audit(dian_audit_table_name,num_fac,process_start_date,customer_registration_name,int(customer_id),issue_date,dian_status)
                            insert_dian_status(dian_status_table_name,num_fac,process_dian_start_date,customer_registration_name,int(customer_id),unique_code_hash,qr_code,issue_date,dian_status,document_type,document_type_abbreviation,path_files,file_name,prefix,val_tot_fac,error_message,xml_document_key_text)
                            print("Termina insert traza")                        


                            application_response_bytes = application_response.encode('utf-8')
                            root_ar = etree.fromstring(application_response_bytes)
                            issue_date_ar = root_ar.find('.//cbc:IssueDate', namespaces=NAMESPACES_AR).text
                            issue_time_ar = root_ar.find('cbc:IssueDate', namespaces=NAMESPACES_AR).text
                            validation_result_code = root_ar.find('cac:DocumentResponse/cac:Response/cbc:ResponseCode', namespaces=NAMESPACES_AR).text
                                                                                                                        
                            supplier_tax_level_code = get_data(root,"supplier","tax_level_code")
                            supplier_tax_id = get_data(root,"supplier","tax_id")
                            supplier_tax_name = get_data(root,"supplier","tax_name")
                            customer_tax_level_code = get_data(root,"customer","tax_level_code")
                            customer_tax_id = get_data(root,"customer","tax_id")
                            customer_tax_name = get_data(root,"customer","tax_name")

                            attach_document = create_attach_document(xml_firm,application_response,num_fac,issue_date,issue_time,issue_date_ar,issue_time_ar,attr_uuid,unique_code_hash,profile_execution_id,validation_result_code,supplier_id,supplier_registration_name,supplier_tax_level_code,supplier_tax_id,supplier_tax_name,customer_id,customer_registration_name,customer_tax_level_code,customer_tax_id,customer_tax_name,str(supplier_scheme_name),str(supplier_scheme_id),str(customer_scheme_name),str(customer_scheme_id))
                            attach_firm = firm_document(attach_document, CERTIFICATE, password_bytes, "third party", now)

                            print("Guardar AttachDocument")
                            save_document(bucket_name_company,path_files,"xml",attach_firm,file_name,"ad")                    
                            
                            response_message = ""
                        else:
                            print("La factura ya fue procesada anteriormente")
                            insert_dian_audit(dian_audit_table_name,num_fac,process_start_date,customer_registration_name,int(customer_id),issue_date,"Reproceso")
                            
                            dian_status = invoice_response["Item"]["dianStatus"]
                            unique_code_hash = invoice_response["Item"]["cufe"]
                            print(f"CUFE{unique_code_hash}")
                            path_files = invoice_response["Item"]["path"]
                            file_name = invoice_response["Item"]["fileName"]
                            xml_document_key = invoice_response["Item"]["xmlDocumentKey"]
                            qr_code = invoice_response["Item"]["qr"]
                            val_tot_fac = invoice_response["Item"]["amount"]
                            if dian_status == "Aprobado" or dian_status == "Rechazado": 
                                print(f"Factura en estado: {dian_status}")
                                firm_key = f"{path_files}/xml/ff{file_name}.xml"
                                appres_key = f"{path_files}/xml/ar{file_name}.xml"
                                
                                #Descargar xml firmado y application response
                                xml_firm = download_file(bucket_name_company,firm_key,True,"UTF-8")
                                application_response = download_file(bucket_name_company,appres_key,True,"UTF-8")                                
                                error_message = invoice_response["Item"]["errorMessage"]                   
                                print("FIN") 
                            elif dian_status == "Timeout":
                                #Consultar la factura en la dian
                                #Proceso sincrono consulta de factura                                
                                '''
                                soap_parameters = {
                                    "trackId": xml_document_key
                                }
                                method = "GetStatus"
                                soap_payload = create_soap_payload(method,WSSE,URL_DIAN,soap_parameters)                                                        
                                
                                #Realizar la carga del XML request a S3
                                print("Inicio de carga de XML request a S3")   
                                print(path_files)                     
                                save_document(bucket_name_company,path_files,"xml",soap_payload,file_name,"reqrep") #Request reproceso
                                '''
                                
                                #realizar el envio del XML a la DIAN
                                c14n_path = f"{path_files}/xml/f2f{file_name}.xml"
                                c14n_xml = download_file(bucket_name_company,c14n_path,True)

                                password_bytes = CERT_PASSWORD.encode('utf-8')
                                xml_firm = firm_document(c14n_xml, CERTIFICATE, password_bytes, "third party", now) 

                                print("Inicio de carga de XML Firmado a S3")                        
                                save_document(bucket_name_company,path_files,"xml",xml_firm,file_name,"ff") #factura firmada                               
                                                                                                
                                #Crear el comprimido del XML y convertir a Base64
                                content_b64 = create_zip(xml_firm,file_name) 
                            
                                print("Inicio de envio del XML a la DIAN")
                                soap_parameters = {
                                    "fileName": f"z{file_name}.zip",
                                    "contentFile": content_b64
                                }
                                method = "SendBillSync"
                                soap_payload = create_soap_payload(method,WSSE,URL_DIAN,soap_parameters) 
                                response_data = get_response_dian(HOST_DIAN, soap_payload)   
                                if response_data is None:
                                    #La dian esta generando timeout   
                                    dian_status = "Timeout"
                                    process_dian_start_date = format_date(datetime.now(ZoneInfo("America/Bogota")))
                                    insert_dian_audit(dian_audit_table_name,num_fac,process_start_date,customer_registration_name,int(customer_id),issue_date,dian_status)
                                    insert_dian_status(dian_status_table_name,num_fac,process_dian_start_date,customer_registration_name,int(customer_id),unique_code_hash,qr_code,issue_date,dian_status,document_type,document_type_abbreviation,path_files,file_name,prefix,val_tot_fac,error_message,"")
                                    #Enviar mensaje a la cola con retardo de X segundos
                                    send_sqs(f"{URL_SQS}{environment}_dian_retryTimeOut",payload,10) #Delay de 10 segundos
                                    message = "TimeOut en la DIAN"
                                    raise ValueError(message)
                                
                                root_response = etree.fromstring(response_data)                                                                        
                                
                                namespaces_response = {
                                    "s": "http://www.w3.org/2003/05/soap-envelope",
                                    "b": "http://schemas.datacontract.org/2004/07/DianResponse",
                                    "c": "http://schemas.microsoft.com/2003/10/Serialization/Arrays",
                                    "": "http://wcf.dian.colombia"  # Espacio de nombres por defecto
                                }
                                print(response_data)

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

                                #Si la factura fue aprobada debo consultar el step del traceabilityProcess para verificar en que punto continuo el proceso
                                #Creo que se debe continuar en la generacion del PDF, debido a que esto es un timeout y solo se necesitaba la respuesta de la DIAN
                                #Para realizar el envio del email

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
                                        save_document(bucket_name_company,path_files,"xml",application_response,file_name,ar_abbreviation)
                                        print("OK")
                                    except Exception as e:                            
                                        print(f"Error al decodificar el contenido Base64: {e}")
                                        raise ValueError(f"Error al decodificar el contenido Base64: {e}")
                                                                
                                    if status_code == "00" or status_code == "99":
                                        print("StatusCode es igual a 0, la factura fue aprobada")
                                        dian_status = "Aprobado"
                                        #Enviar a crear el PDF
                                        print("Enviando datos para crear PDF")
                                        #Obtener el payload del PDF desde S3
                                        data_path = f"{path_files}/json/{file_name}.json"
                                        message = download_file(bucket_name_company,data_path,True)
                                        send_sqs(f"{URL_SQS}{environment}_Pdf_Create_Ondemand",message)
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

                                #Insertar en dian process el estado de la validacion ante la DIAN
                                process_dian_start_date = format_date(datetime.now(ZoneInfo("America/Bogota")))
                                insert_dian_audit(dian_audit_table_name,num_fac,process_start_date,customer_registration_name,int(customer_id),issue_date,dian_status)

                                insert_dian_status(dian_status_table_name,num_fac,process_dian_start_date,customer_registration_name,int(customer_id),unique_code_hash,qr_code,issue_date,dian_status,document_type,document_type_abbreviation,path_files,file_name,prefix,val_tot_fac,error_message,xml_document_key_text)
                                print("Termina insert traza")                        


                                application_response_bytes = application_response.encode('utf-8')
                                root_ar = etree.fromstring(application_response_bytes)
                                issue_date_ar = root_ar.find('.//cbc:IssueDate', namespaces=NAMESPACES_AR).text
                                issue_time_ar = root_ar.find('cbc:IssueDate', namespaces=NAMESPACES_AR).text
                                validation_result_code = root_ar.find('cac:DocumentResponse/cac:Response/cbc:ResponseCode', namespaces=NAMESPACES_AR).text
                                                                                                                            
                                supplier_tax_level_code = get_data(root,"supplier","tax_level_code")
                                supplier_tax_id = get_data(root,"supplier","tax_id")
                                supplier_tax_name = get_data(root,"supplier","tax_name")
                                customer_tax_level_code = get_data(root,"customer","tax_level_code")
                                customer_tax_id = get_data(root,"customer","tax_id")
                                customer_tax_name = get_data(root,"customer","tax_name")

                                attach_document = create_attach_document(xml_firm,application_response,num_fac,issue_date,issue_time,issue_date_ar,issue_time_ar,attr_uuid,unique_code_hash,profile_execution_id,validation_result_code,supplier_id,supplier_registration_name,supplier_tax_level_code,supplier_tax_id,supplier_tax_name,customer_id,customer_registration_name,customer_tax_level_code,customer_tax_id,customer_tax_name,str(supplier_scheme_name),str(supplier_scheme_id),str(customer_scheme_name),str(customer_scheme_id))
                                attach_firm = firm_document(attach_document, CERTIFICATE, password_bytes, "third party", now)

                                print("Guardar AttachDocument")
                                save_document(bucket_name_company,path_files,"xml",attach_firm,file_name,"ad")                    
                                
                                response_message = ""
                                    
                    else:
                        message = "Contraseña incorrecta"
                        print(message)
                        raise ValueError(message)
            else:
                message = "No se encontró el nit enviado en la tabla de usuarios"
                print(message)
                raise ValueError(message)              

        else:
            message = "No se encontró el nit enviado en la tabla de usuarios"
            print(message)
            raise ValueError(message) 
    
    except Exception as e:
        response_message = str(e)

    finally:
        response_service["codigo"] = code
        response_service["mensaje"] = response_message
        response_service["validaciones"] = validations
        response_service["documento"] = xml_firm
        response_service["applicationResponse"] = application_response
        response_service["ErrorMessage"] = error_message
        response_service["cufe"] = unique_code_hash
        response_service["qrdata"] = qr_code
        response_service["token"] = ""
        return response_service