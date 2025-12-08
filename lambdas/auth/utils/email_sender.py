import boto3
import os
import logging
from botocore.exceptions import ClientError

ses_client = boto3.client('ses', region_name='ap-northeast-2')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

SENDER_EMAIL = os.environ['SES_FROM_EMAIL']
FRONTEND_URL = os.environ['FRONTEND_URL']
API_BASE_URL = os.environ['API_BASE_URL']

def send_verification_email(email: str, token: str):
  if not SENDER_EMAIL or not API_BASE_URL:
    logger.error("SES_FROM_EMAIL or API_BASE_URL is not set")
    raise ValueError("SES_FROM_EMAIL or API_BASE_URL is not set")
  
  verification_link = f"{API_BASE_URL}/verify-email?token={token}"

  try:
    response = ses_client.send_email(
      Source=SENDER_EMAIL,
      Destination={'ToAddresses': [email]},
      Message={
        'Subject': {'Data': 'CtrlF4 회원가입 이메일 인증 확인', 'Charset': 'UTF-8'},
        'Body': {
          'Text': {
            'Data': f"이메일 인증을 위해 아래 링크를 클릭해주세요:\n{verification_link}",
            'Charset': 'UTF-8'
          },
          'Html': {
            'Data': f"<p>이메일 인증을 위해 아래 링크를 클릭해주세요:</p><a href='{verification_link}'>이메일 인증</a>",
            'Charset': 'UTF-8'
          }
        },
      }
    )
    logger.info(f"Verification email sent to {email}")
    return response
  except ClientError as e:
    logger.error(f"Error sending verification email: {e}")
    raise e

def send_password_reset_email(email: str, token: str):
  
  if not SENDER_EMAIL or not FRONTEND_URL:
    logger.error("SES_FROM_EMAIL or FRONTEND_URL is not set")
    raise ValueError("SES_FROM_EMAIL or FRONTEND_URL is not set")
  
  reset_link = f"{FRONTEND_URL}/reset-password?token={token}"

  try:
    response = ses_client.send_email(
      Source=SENDER_EMAIL,
      Destination={'ToAddresses': [email]},
      Message={
        'Subject': {'Data': 'CtrlF4 비밀번호 재설정', 'Charset': 'UTF-8'},
        'Body': {
          'Text': {
            'Data': f"비밀번호를 재설정하려면 아래 링크를 클릭해주세요:\n{reset_link}\n\n이 링크는 1시간 후에 만료됩니다.",
            'Charset': 'UTF-8'
          },
          'Html': {
            'Data': f"<p>비밀번호를 재설정하려면 아래 링크를 클릭해주세요:</p><a href='{reset_link}'>비밀번호 재설정</a><p>이 링크는 1시간 후에 만료됩니다.</p>",
            'Charset': 'UTF-8'
          }
        },
      }
    )
    logger.info(f"Password reset email sent to {email}")
    return response
  except ClientError as e:
    logger.error(f"Error sending password reset email: {e}")
    raise e