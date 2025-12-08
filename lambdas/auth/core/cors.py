import os

# 프론트엔드 URL (환경변수에서 가져오거나 기본값 사용)
FRONTEND_URL = os.environ.get('FRONTEND_URL', 'https://main.dge3kp0b4w7rm.amplifyapp.com')

def get_cors_headers() -> dict:

  return {
    'Access-Control-Allow-Origin': FRONTEND_URL,
    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, Cookie',
    'Access-Control-Allow-Credentials': 'true',
    'Access-Control-Expose-Headers': 'Set-Cookie'
  }

def add_cors_headers(response: dict) -> dict:
  if 'headers' not in response:
    response['headers'] = {}
  
  # 기존 헤더와 CORS 헤더 병합
  response['headers'].update(get_cors_headers())
  
  return response

