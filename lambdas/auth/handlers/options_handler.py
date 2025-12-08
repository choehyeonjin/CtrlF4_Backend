"""
OPTIONS 요청을 처리하는 핸들러 (CORS preflight)
"""
from src.core.cors import get_cors_headers

def handler(event, context):
  """
  CORS preflight 요청을 처리합니다.
  """
  return {
    'statusCode': 200,
    'headers': get_cors_headers(),
    'body': ''
  }

