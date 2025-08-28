import json
import boto3

eventbridge = boto3.client('events')

def manage_rule(rule_name, action):
    if action == "enable":
        eventbridge.enable_rule(Name=rule_name)
        return f"Regla {rule_name} activada"
    elif action == "disable":
        eventbridge.disable_rule(Name=rule_name)
        return f"Regla {rule_name} desactivada"
    else:
        return "Acción inválida, usa 'enable' o 'disable'"

def check_rule_status(rule_name):
    try:
        response = eventbridge.describe_rule(Name=rule_name)
        return f"Estado de la regla '{rule_name}': {response['State']}"  # Puede ser 'ENABLED' o 'DISABLED'
    except Exception as e:
        return f"Error al consultar la regla: {str(e)}"

def lambda_handler(event, context):
    rule_name = "Automatic_retries_invoice"

    print(check_rule_status(rule_name))
    
    action = "disable"  # Cambia a "enable" para activarla
    #action = "enable"  # Cambia a "disable" para desactivarla
    print(manage_rule(rule_name, action))
