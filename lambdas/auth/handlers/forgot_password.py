import json
import logging
from src.core.data_access import get_user_by_email_for_reset
from src.core.security import create_password_reset_token
from src.utils.email_sender import send_password_reset_email
from src.core.cors import add_cors_headers

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
  try:
    body = json.loads(event['body'])
    email = body.get('email', '').strip().lower()
    
    if not email:
      return add_cors_headers({
        'statusCode': 400,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': '이메일을 입력해주세요.'}, ensure_ascii=False)
      })
    
    # 사용자 조회
    user = get_user_by_email_for_reset(email)
    
    # 보안을 위해 사용자가 존재하지 않아도 성공 메시지 반환
    if not user:
      response = {
        'statusCode': 200,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({
          'message': '입력하신 이메일로 비밀번호 재설정 링크를 발송했습니다.'
        }, ensure_ascii=False)
      }
      return add_cors_headers(response)
    
    # 사용자가 비활성화된 경우
    if not user['is_active']:
      response = {
        'statusCode': 200,
        'headers': {
          'Content-Type': 'application/json'
        },
        'body': json.dumps({
          'message': '입력하신 이메일로 비밀번호 재설정 링크를 발송했습니다.'
        }, ensure_ascii=False)
      }
      return add_cors_headers(response)
    
    # 비밀번호 재설정 토큰 생성
    reset_token = create_password_reset_token(user['id'])
    
    # 이메일 발송
    try:
      send_password_reset_email(user['email'], reset_token)
      logger.info(f"Password reset email sent to {user['email']}")
    except Exception as e:
      logger.error(f"Failed to send password reset email: {e}")
      # 이메일 발송 실패해도 보안상 성공 메시지 반환
    
    response = {
      'statusCode': 200,
      'headers': {
        'Content-Type': 'application/json'
      },
      'body': json.dumps({
        'message': '입력하신 이메일로 비밀번호 재설정 링크를 발송했습니다.'
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
    logger.error(f"Error during password reset request: {e}")
    return add_cors_headers({
      'statusCode': 500,
      'headers': {
        'Content-Type': 'application/json'
      },
      'body': json.dumps({'message': '비밀번호 재설정 요청 중 오류가 발생했습니다.'}, ensure_ascii=False)
    })
