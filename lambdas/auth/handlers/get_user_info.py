import json
import logging
import jwt
import os
from src.core.data_access import get_user_by_user_id
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
    
    try:
      payload = jwt.decode(token, os.environ['JWT_SECRET_KEY'], algorithms=['HS256'])
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
    
    # 사용자 정보 조회
    user = get_user_by_user_id(user_id)
    if not user:
      return add_cors_headers({
        'statusCode': 404,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '사용자를 찾을 수 없습니다.'}, ensure_ascii=False)
      })
    
    # 비밀번호는 제외하고 사용자 정보 반환
    user_info = {
      'id': user['id'],
      'email': user['email'],
      'nickname': user['nickname'],
      'is_active': user['is_active']
    }
    
    return add_cors_headers({
      'statusCode': 200,
      'headers': {
        'Content-Type': 'application/json'
      },
      'body': json.dumps(user_info, ensure_ascii=False)
    })
    
  except Exception as e:
    logger.error(f"Error during user info retrieval: {e}")
    return add_cors_headers({
      'statusCode': 500,
      'headers': {
        'Content-Type': 'application/json'
      },
      'body': json.dumps({'message': '사용자 정보 조회 중 오류가 발생했습니다.'}, ensure_ascii=False)
    })
