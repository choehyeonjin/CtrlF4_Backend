import json
import logging
import jwt
import os
from src.core.data_access import update_user_nickname
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
    
    # 요청 본문에서 새 닉네임 추출
    body = json.loads(event['body'])
    new_nickname = body.get('nickname', '').strip()
    
    if not new_nickname:
      return add_cors_headers({
        'statusCode': 400,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '닉네임을 입력해주세요.'}, ensure_ascii=False)
      })
    
    if len(new_nickname) < 2:
      return add_cors_headers({
        'statusCode': 400,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '닉네임은 2자 이상이어야 합니다.'}, ensure_ascii=False)
      })
    
    if len(new_nickname) > 20:
      return add_cors_headers({
        'statusCode': 400,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '닉네임은 20자 이하여야 합니다.'}, ensure_ascii=False)
      })
    
    # 닉네임 업데이트
    update_user_nickname(user_id, new_nickname)
    
    return add_cors_headers({
      'statusCode': 200,
      'headers': {
        'Content-Type': 'application/json'
      },
      'body': json.dumps({
        'message': '닉네임이 성공적으로 변경되었습니다.',
        'nickname': new_nickname,
      }, ensure_ascii=False)
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
    logger.error(f"Error during nickname update: {e}")
    return add_cors_headers({
      'statusCode': 500,
      'headers': {
        'Content-Type': 'application/json'
      },
      'body': json.dumps({'message': '닉네임 변경 중 오류가 발생했습니다.'}, ensure_ascii=False)
    })
