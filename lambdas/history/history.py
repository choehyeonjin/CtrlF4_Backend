import json
import logging
import jwt
import os
from src.core.data_access import get_user_history
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
    
    # 사용자 히스토리 조회
    history = get_user_history(user_id)
    
    return add_cors_headers({
      'statusCode': 200,
      'headers': {
        'Content-Type': 'application/json'
      },
      'body': json.dumps({
        'history': history
      }, ensure_ascii=False)
    })
    
  except Exception as e:
    logger.error(f"Error during history retrieval: {e}")
    return add_cors_headers({
      'statusCode': 500,
      'headers': {
        'Content-Type': 'application/json'
      },
      'body': json.dumps({'message': '히스토리 조회 중 오류가 발생했습니다.'}, ensure_ascii=False)
    })

