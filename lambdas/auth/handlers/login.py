import json
import logging
from src.core.data_access import get_user_by_email, store_refresh_token
from src.core.security import verify_password, create_access_token, create_refresh_token
from src.core.cors import add_cors_headers

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
  try:
    body = json.loads(event['body'])
    email = body['email']
    password = body['password']

    user = get_user_by_email(email)
    
    if not user or not verify_password(password, user['hashed_password']):
      return add_cors_headers({
        'statusCode': 401,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '이메일 또는 비밀번호가 일치하지 않습니다.'}, ensure_ascii=False)
      })
    
    if not user['is_active']:
      return add_cors_headers({
        'statusCode': 401,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '이메일 인증이 완료되지 않았습니다.'}, ensure_ascii=False)
      })
    
    access_token = create_access_token(user['id'])
    refresh_token_raw, refresh_token_hash = create_refresh_token()
    
    store_refresh_token(user['id'], refresh_token_hash)

    # refresh_token을 httpOnly 쿠키로 설정
    # 쿠키 만료 시간: 30일 (refresh token 만료 시간과 동일하게 설정)
    cookie_max_age = 30 * 24 * 60 * 60  # 30일을 초 단위로 변환
    cookie_value = f"refresh_token={refresh_token_raw}; Path=/; HttpOnly; SameSite=None; Secure; Max-Age={cookie_max_age}"
    
    response = {
      'statusCode': 200,
      'headers': {
        'Content-Type': 'application/json',
        'Set-Cookie': cookie_value
      },
      'body': json.dumps({
        'access_token': access_token,
        'email': user['email'],
        'nickname': user['nickname'],
        'id': user['id']
      }, ensure_ascii=False)
    }
    
    return add_cors_headers(response)
  except json.JSONDecodeError:
    return add_cors_headers({
      'statusCode': 400,
      'headers': {
        'Content-Type': 'application/json'
      },
      'body': json.dumps({'message': '잘못된 JSON 형식입니다.'}, ensure_ascii=False)
    })
  except Exception as e:
    logger.error(f"Error during login: {e}")
    return add_cors_headers({
      'statusCode': 500,
      'headers': {
        'Content-Type': 'application/json'
      },
      'body': json.dumps({'message': '로그인 중 오류가 발생했습니다.'}, ensure_ascii=False)
    })

