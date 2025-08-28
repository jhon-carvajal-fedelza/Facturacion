import boto3
import json
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')

def query_dynamo(table, index_name=None, key_condition=None, filter_conditions=None):
    params = {}
    if index_name:
        params['IndexName'] = index_name
    if key_condition:
        params['KeyConditionExpression'] = key_condition
    if filter_conditions:
        params['FilterExpression'] = filter_conditions
    return table.query(**params)

def lambda_handler(event, context):
    environment = context.invoked_function_arn.split(":")[-1]
    print(event)
    supplier_id = event.get('supplierId')
    num_fac = event.get('numFac')
    cufe = event.get('cufe')
    customer_id = event.get('customerId')
    customer_name = event.get('customerName')
    prefix = event.get('prefix')
    init_invoice_date = event.get('initInvoiceDate')
    end_invoice_date = event.get('endInvoiceDate')

    print(f"supplier_id: {supplier_id}")
    print(f"num_fac: {num_fac}")
    print(f"cufe: {cufe}")
    print(f"customer_id: {customer_id}")
    print(f"customer_name: {customer_name}")
    print(f"prefix: {prefix}")
    print(f"init_invoice_date: {init_invoice_date}")
    print(f"end_invoice_date: {end_invoice_date}")
    
    dian_process_table_name = f"{environment}_{supplier_id}_dianProcess"
    dian_status_table_name = f"{environment}_{supplier_id}_dianStatus"
    dian_process_table = dynamodb.Table(dian_process_table_name)
    dian_status_table = dynamodb.Table(dian_status_table_name)

    response_data = {}
    
    # Consulta en dianProcess
    key_condition = None
    index_name = None
    filter_conditions = None

    #Se da prioridad de la siguiente manera:
    #1. Numero de factura
    #2. CUFE
    #3. Nit o cedula del cliente
    #4. Nombre del cliente
    #5. Prefijo
    #Por ultimo se agregan fechas si existen
    if num_fac and num_fac.strip():
        print("Realizar consulta por num_fac")
        key_condition = Key('numFac').eq(num_fac)
    elif cufe:
        print("Realizar consulta por cufe")
        key_condition = Key('cufe').eq(cufe)
        index_name = "GSI_cufe"
    elif customer_id:
        print("Realizar consulta por customer_id")
        key_condition = Key('customerId').eq(customer_id)
        index_name = "GSI_customerId"
    elif customer_name:
        print("Realizar consulta por customer_name")
        key_condition = Key('customerName').eq(customer_name)
        index_name = "GSI_customerName"
    elif prefix:
        print("Realizar consulta por prefix")
        key_condition = Key('prefix').eq(prefix)
        index_name = "GSI_prefix"
    
    if init_invoice_date and end_invoice_date:
        filter_conditions = Attr('invoiceDate').between(init_invoice_date, end_invoice_date)
    
    if key_condition:
        dian_result = query_dynamo(dian_process_table, index_name, key_condition, filter_conditions)
        '''
        dian_result = dian_process_table.query(
            KeyConditionExpression=key_conditions[0],
            FilterExpression=filter_conditions if filter_conditions else None
        )
        '''
    
    # Consulta en estadoEnvio
    '''
    key_conditions = []
    filter_conditions = None
    if num_fac:
        key_conditions.append(Key('num_fac').eq(num_fac))
    if customer_nit:
        key_conditions.append(Key('customer_nit').eq(customer_nit))
    if init_date and end_date:
        filter_conditions = Attr('date').between(init_date, end_date)
    
    if key_conditions:
        estado_result = estado_envio_table.query(
            KeyConditionExpression=key_conditions[0] if len(key_conditions) == 1 else key_conditions[0] & key_conditions[1],
            FilterExpression=filter_conditions if filter_conditions else None,
            ProjectionExpression='statusSend, date'
        )
        response_data['estadoEnvio'] = estado_result.get('Items', [])
    '''

    return dian_result