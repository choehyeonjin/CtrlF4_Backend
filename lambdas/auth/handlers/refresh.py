import json
import logging
import hashlib
from datetime import datetime
from src.core.data_access import get_refresh_token_by_hash, cleanup_expired_tokens, get_user_by_user_id
from src.core.security import create_access_token, create_refresh_token
from src.core.cors import add_cors_headers

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def hash_refresh_token(token: str) -> str:
  return hashlib.sha256(token.encode()).hexdigest()

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
    
    if not refresh_token:
      return add_cors_headers({
        'statusCode': 400,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '리프레시 토큰이 필요합니다.'}, ensure_ascii=False)
      })
    
    # 리프레시 토큰 해시화
    refresh_token_hash = hash_refresh_token(refresh_token)
    
    # 데이터베이스에서 토큰 조회
    token_data = get_refresh_token_by_hash(refresh_token_hash)
    
    if not token_data:
      return add_cors_headers({
        'statusCode': 401,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '유효하지 않은 리프레시 토큰입니다.'}, ensure_ascii=False)
      })
    
    # 토큰이 무효화되었거나 만료되었는지 확인
    if token_data['is_revoked']:
      return add_cors_headers({
        'statusCode': 401,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '이미 사용된 리프레시 토큰입니다.'}, ensure_ascii=False)
      })
    
    if token_data['expires_at'].replace(tzinfo=None) < datetime.utcnow():
      return add_cors_headers({
        'statusCode': 401,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '리프레시 토큰이 만료되었습니다.'}, ensure_ascii=False)
      })
    
    # 새로운 액세스 토큰 생성
    new_access_token = create_access_token(token_data['user_id'])
    
    # 새로운 리프레시 토큰 생성 (선택적 - 보안을 위해 토큰 로테이션)
    new_refresh_token, new_refresh_token_hash = create_refresh_token()
    
    # 기존 리프레시 토큰 무효화
    from src.core.data_access import revoke_refresh_token
    revoke_refresh_token(refresh_token_hash)
    
    # 새로운 리프레시 토큰 저장
    from src.core.data_access import store_refresh_token
    store_refresh_token(token_data['user_id'], new_refresh_token_hash)
    
    # 만료된 토큰 정리
    cleanup_expired_tokens()
    
    # 사용자 정보 조회
    user = get_user_by_user_id(token_data['user_id'])
    if not user:
      return add_cors_headers({
        'statusCode': 404,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '사용자를 찾을 수 없습니다.'}, ensure_ascii=False)
      })
    
    # 새로운 refresh_token을 httpOnly 쿠키로 설정
    cookie_max_age = 30 * 24 * 60 * 60  # 30일
    cookie_value = f"refresh_token={new_refresh_token}; Path=/; HttpOnly; SameSite=None; Secure; Max-Age={cookie_max_age}"
    
    response = {
      'statusCode': 200,
      'headers': {
        'Content-Type': 'application/json',
        'Set-Cookie': cookie_value
      },
      'body': json.dumps({
        'access_token': new_access_token,
        'email': user['email'],
        'nickname': user['nickname'],
        'id': user['id']
      }, ensure_ascii=False)
    }
    
    return add_cors_headers(response)
    
  except Exception as e:
    logger.error(f"Error during refresh: {e}")
    return add_cors_headers({
      'statusCode': 500,
      'headers': {
        'Content-Type': 'application/json'
      },
      'body': json.dumps({'message': '리프레시 토큰 갱신 중 오류가 발생했습니다.'}, ensure_ascii=False)
    })