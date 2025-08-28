import boto3
import os
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource('dynamodb')
REGION = 'us-east-1'

def count_and_sum_by_type_and_status(table, doc_types, status_filter):
    count = 0
    total = 0.0
    last_evaluated_key = None

    filter_expr = Attr("documentTypeAbbreviation").is_in(doc_types) & Attr("dianStatus").is_in(status_filter)

    while True:
        scan_kwargs = {
            "FilterExpression": filter_expr,
            "ProjectionExpression": "amount",
        }
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

        response = table.scan(**scan_kwargs)
        count += len(response.get("Items", []))
        for item in response.get("Items", []):
            try:
                total += float(item.get("amount", 0))
            except:
                pass

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    return count, total

def lambda_handler(event, context):
    nit = event.get("identificationNumber")
    environment = context.invoked_function_arn.split(":")[-1]
    #Pruebas
    environment = "Dev"
    table_name = f"{environment}_{nit}_dianStatus"
    print(table_name)
    table = dynamodb.Table(table_name)

    try:
        # Categorías
        tipos_facturas = ["fv"]
        tipos_notas = ["nc", "nd", "nas", "ncs"]
        tipos_documentos = ["ds"]

        # Facturas
        fact_acept, val_fact_acept = count_and_sum_by_type_and_status(table, tipos_facturas, ["Aprobado"])
        fact_rech, val_fact_rech = count_and_sum_by_type_and_status(table, tipos_facturas, ["Rechazado","Timeout"])

        # Notas
        nota_acept, val_nota_acept = count_and_sum_by_type_and_status(table, tipos_notas, ["Aprobado"])
        nota_rech, val_nota_rech = count_and_sum_by_type_and_status(table, tipos_notas, ["Rechazado","Timeout"])

        # Documentos soporte
        doc_acept, val_doc_acept = count_and_sum_by_type_and_status(table, tipos_documentos, ["Aprobado"])
        doc_rech, val_doc_rech = count_and_sum_by_type_and_status(table, tipos_documentos, ["Rechazado","Timeout"])

        val_total_aceptadas = round(val_fact_acept + val_nota_acept + val_doc_acept, 2)
        val_total_rechazadas = round(val_fact_rech + val_nota_rech + val_doc_rech, 2)

        return {
            "totalFacturas": fact_acept + fact_rech,
            "totalNotas": nota_acept + nota_rech,
            "totalDocumentos": doc_acept + doc_rech,
            "facturasAceptadas": fact_acept,
            "facturasRechazadas": fact_rech,
            "notasAceptadas": nota_acept,
            "notasRechazadas": nota_rech,
            "documentosAceptados": doc_acept,
            "documentosRechazados": doc_rech,
            "valorTotal": round(val_total_aceptadas + val_total_rechazadas, 2)
        }

    except Exception as e:
        return {"error": str(e)}