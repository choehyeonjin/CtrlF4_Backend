import json
import logging
from src.core.data_access import revoke_refresh_token
from src.core.cors import add_cors_headers

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
  try:
    # 쿠키에서 refresh_token 추출
    refresh_token = None
    cookies = event.get('headers', {}).get('Cookie', '') or event.get('headers', {}).get('cookie', '')
    
    if cookies:
      for cookie in cookies.split(';'):
        cookie = cookie.strip()
        if cookie.startswith('refresh_token='):
          refresh_token = cookie.split('=', 1)[1]
          break
    
    if refresh_token:
      # 리프레시 토큰을 해시로 변환
      import hashlib
      refresh_token_hash = hashlib.sha256(refresh_token.encode()).hexdigest()
      
      # 리프레시 토큰 무효화
      revoke_refresh_token(refresh_token_hash)
    
    # 쿠키 삭제를 위해 만료된 쿠키 설정
    cookie_value = "refresh_token=; Path=/; HttpOnly; SameSite=None; Secure; Max-Age=0"
    
    response = {
      'statusCode': 200,
      'headers': {
        'Content-Type': 'application/json',
        'Set-Cookie': cookie_value
      },
      'body': json.dumps({'message': '로그아웃이 완료되었습니다.'}, ensure_ascii=False)
    }
    
    return add_cors_headers(response)
  except Exception as e:
    logger.error(f"Error during logout: {e}")
    return add_cors_headers({
      'statusCode': 500,
      'headers': {
        'Content-Type': 'application/json'
      },
      'body': json.dumps({'message': '로그아웃 중 오류가 발생했습니다.'}, ensure_ascii=False)
    })
