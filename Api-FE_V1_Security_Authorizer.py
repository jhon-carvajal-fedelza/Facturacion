import os
import jwt

SECRET_KEY = os.environ.get("JWT_SECRET")

def lambda_handler(event, context):
    token = event["authorizationToken"]
    method_arn = event["methodArn"]
    if not token.startswith("Bearer "):
        raise Exception("Unauthorized")

    jwt_token = token.split(" ")[1]

    print(jwt_token)
    try:
        decoded = jwt.decode(jwt_token, SECRET_KEY, algorithms=["HS256"])
        print(decoded)
        # Aquí puedes validar claims adicionales si quieres
    except jwt.ExpiredSignatureError:
        raise Exception("Unauthorized - Token expired")
    except jwt.InvalidTokenError:
        raise Exception("Unauthorized - Invalid token")

    return {
        "principalId": decoded.get("sub", "user"),
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [{
                "Action": "execute-api:Invoke",
                "Effect": "Allow",
                "Resource": method_arn
            }]
        },
        "context": decoded  # opcional: pasar datos al endpoint real
    }