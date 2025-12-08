import json
import logging
import jwt
import os
from src.core.data_access import get_user_by_user_id, delete_user
from src.core.cors import add_cors_headers

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
  try:
    # JWT 토큰 검증
    auth_header = event.get('headers', {}).get('Authorization', '')
    if not auth_header.startswith('Bearer '):
      return add_cors_headers({
        'statusCode': 401,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '인증 토큰이 필요합니다.'}, ensure_ascii=False)
      })
    
    token = auth_header.split(' ')[1]
    
    # 디버깅을 위한 로그 추가
    logger.info(f"JWT_SECRET_KEY exists: {bool(os.environ.get('JWT_SECRET_KEY'))}")
    logger.info(f"Token length: {len(token)}")
    
    try:
      payload = jwt.decode(token, os.environ['JWT_SECRET_KEY'], algorithms=['HS256'])
      logger.info(f"Token decoded successfully: {payload}")
      if payload.get('type') != 'access':
        return add_cors_headers({
          'statusCode': 401,
          'headers': {
            'Content-Type': 'application/json'
          },
          'body': json.dumps({'message': '타입이 다른 토큰입니다.'}, ensure_ascii=False)
        })
      user_id = payload['sub']
    except jwt.InvalidTokenError as e:
      logger.error(f"JWT decode error: {e}")
      return add_cors_headers({
        'statusCode': 401,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '토큰 디코딩 오류가 발생했습니다.'}, ensure_ascii=False)
      })
    
    body = json.loads(event['body'])
    password = body['password']
    
    # JWT에서 가져온 user_id로 사용자 확인
    user = get_user_by_user_id(user_id)
    if not user:
      return add_cors_headers({
        'statusCode': 404,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '사용자를 찾을 수 없습니다.'}, ensure_ascii=False)
      })
    
    # 비밀번호 확인
    from src.core.security import verify_password
    if not verify_password(password, user['hashed_password']):
      return add_cors_headers({
        'statusCode': 401,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '비밀번호가 일치하지 않습니다.'}, ensure_ascii=False)
      })
    
    # 사용자 삭제 (CASCADE로 관련 데이터도 함께 삭제)
    delete_user(user['id'])
    
    return add_cors_headers({
      'statusCode': 200,
      'headers': {
        'Content-Type': 'application/json'
      },
      'body': json.dumps({'message': '회원 탈퇴가 완료되었습니다.'}, ensure_ascii=False)
    })
  except json.JSONDecodeError:
    return add_cors_headers({
      'statusCode': 400,
      'headers': {
        'Content-Type': 'application/json'
      },
      'body': json.dumps({'message': '잘못된 JSON 형식입니다.'}, ensure_ascii=False)
    })
  except Exception as e:
    logger.error(f"Error during account deletion: {e}")
    return add_cors_headers({
      'statusCode': 500,
      'headers': {
        'Content-Type': 'application/json'
      },
      'body': json.dumps({'message': '회원 탈퇴 중 오류가 발생했습니다.'}, ensure_ascii=False)
    })
