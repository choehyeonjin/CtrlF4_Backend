import json
import logging
import jwt
import os
from src.core.data_access import get_user_by_user_id, update_user_password
from src.core.security import hash_password, verify_password
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
    
    # 요청 본문에서 비밀번호 추출
    body = json.loads(event['body'])
    current_password = body.get('current_password', '').strip()
    new_password = body.get('new_password', '').strip()
    
    if not current_password:
      return add_cors_headers({
        'statusCode': 400,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '현재 비밀번호를 입력해주세요.'}, ensure_ascii=False)
      })
    
    if not new_password:
      return add_cors_headers({
        'statusCode': 400,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '새 비밀번호를 입력해주세요.'}, ensure_ascii=False)
      })
    
    if len(new_password) < 8:
      return add_cors_headers({
        'statusCode': 400,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '새 비밀번호는 8자 이상이어야 합니다.'}, ensure_ascii=False)
      })
    
    if current_password == new_password:
      return add_cors_headers({
        'statusCode': 400,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '새 비밀번호는 현재 비밀번호와 달라야 합니다.'}, ensure_ascii=False)
      })
    
    # 사용자 조회
    user = get_user_by_user_id(user_id)
    if not user:
      return add_cors_headers({
        'statusCode': 404,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '사용자를 찾을 수 없습니다.'}, ensure_ascii=False)
      })
    
    # 현재 비밀번호 확인
    if not verify_password(current_password, user['hashed_password']):
      return add_cors_headers({
        'statusCode': 401,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '현재 비밀번호가 일치하지 않습니다.'}, ensure_ascii=False)
      })
    
    # 새 비밀번호 해시화 및 업데이트
    hashed_password = hash_password(new_password)
    update_user_password(user_id, hashed_password)
    
    return add_cors_headers({
      'statusCode': 200,
      'headers': {
        'Content-Type': 'application/json'
      },
      'body': json.dumps({
        'message': '비밀번호가 성공적으로 변경되었습니다.'
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
    logger.error(f"Error during password change: {e}")
    return add_cors_headers({
      'statusCode': 500,
      'headers': {
        'Content-Type': 'application/json'
      },
      'body': json.dumps({'message': '비밀번호 변경 중 오류가 발생했습니다.'}, ensure_ascii=False)
    })
