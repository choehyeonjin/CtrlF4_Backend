import json
import logging
from src.core.data_access import create_user, get_user_by_email
from src.core.security import hash_password, create_verification_token
from src.utils.email_sender import send_verification_email
from src.core.cors import add_cors_headers

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
  try:
    body = json.loads(event['body'])
    nickname = body['nickname']
    email = body['email']
    password = body['password']

    if get_user_by_email(email):
      return add_cors_headers({
        'statusCode': 409,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '이미 존재하는 이메일입니다.'}, ensure_ascii=False)
      })

    hashed_pw = hash_password(password)
    user_id = create_user(email, hashed_pw, nickname)
    
    # verification_token = create_verification_token(user_id)
    # send_verification_email(email, verification_token)

    response = {
      'statusCode': 200,
      'headers': {
        'Content-Type': 'application/json'
      },
      'body': json.dumps({
        'message': '회원가입이 완료되었습니다. 이메일 인증 후 로그인 가능합니다.',
        # 'verification_token': verification_token,
        'user_id': user_id
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
    logger.error(f"Error during registration: {e}")
    response = {
      'statusCode': 500,
      'headers': {
        'Content-Type': 'application/json'
      },
      'body': json.dumps({
        'message': f'회원가입 중 오류가 발생했습니다. 오류 상세: {str(e)}',
        'status': 'error',
        'error_type': type(e).__name__,
        'timestamp': context.aws_request_id if context else 'unknown'
      }, ensure_ascii=False)
    }
    return add_cors_headers(response)