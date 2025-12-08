import json
import jwt
import os
import logging
from src.core.data_access import activate_user
from src.core.cors import add_cors_headers

logger = logging.getLogger()
logger.setLevel(logging.INFO)

FRONTEND_URL = os.environ.get('FRONTEND_URL', 'http://localhost:3000')
      
def handler(event, context):
  try:
    token = event['queryStringParameters']['token']


    payload = jwt.decode(token, os.environ['JWT_SECRET_KEY'], algorithms=['HS256'])
    
    if payload.get('type') != 'email_verification':
      # 실패 시 프론트엔드 페이지로 리다이렉트
      response = {
        'statusCode': 302,
        'headers': {
          'Location': f'{FRONTEND_URL}/email-verification-fail'
        }
      }
      return add_cors_headers(response)

    user_id = payload['sub']

    activate_user(user_id)

    # 성공 시 프론트엔드 페이지로 리다이렉트
    response = {
      'statusCode': 302,
      'headers': {
        'Location': f'{FRONTEND_URL}/email-verification-success'
      }
    }
    return add_cors_headers(response)
  except Exception as e:
    logger.error(f"Error during email verification: {e}")
    # 예외 발생 시에도 실패 페이지로 리다이렉트
    response = {
      'statusCode': 302,
      'headers': {
        'Location': f'{FRONTEND_URL}/email-verification-fail'
      }
    }
    return add_cors_headers(response)