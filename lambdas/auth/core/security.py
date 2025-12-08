import bcrypt
import jwt
import os
from datetime import datetime, timedelta

def hash_password(password: str) -> str:
  salt = bcrypt.gensalt()
  hashed_pw = bcrypt.hashpw(password.encode('utf-8'), salt)
  return hashed_pw.decode('utf-8')

def verify_password(plain_password: str, hashed_password: str) -> bool:
  return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))

def create_verification_token(user_id: str) -> str:
  payload = {
    'sub': user_id,
    'exp': datetime.utcnow() + timedelta(hours=1),
    'type': 'email_verification'
  }
  return jwt.encode(payload, os.environ['JWT_SECRET_KEY'], algorithm='HS256')

def create_access_token(user_id: str) -> str:
  payload = {
    'sub': user_id,
    'exp': datetime.utcnow() + timedelta(hours=1),
    'type': 'access'
  }
  return jwt.encode(payload, os.environ['JWT_SECRET_KEY'], algorithm='HS256')

def create_refresh_token() -> tuple[str, str]:
  import secrets
  import hashlib
  
  # 쿠키 저장용 리프레시 토큰 생성
  refresh_token_raw = secrets.token_urlsafe(32)
  
  # 디비 저장용 리프레시 토큰 해시 생성
  refresh_token_hash = hashlib.sha256(refresh_token_raw.encode()).hexdigest()
  
  return refresh_token_raw, refresh_token_hash

def create_password_reset_token(user_id: str) -> str:
  payload = {
    'sub': user_id,
    'exp': datetime.utcnow() + timedelta(hours=1),  # 1시간 유효
    'type': 'password_reset'
  }
  return jwt.encode(payload, os.environ['JWT_SECRET_KEY'], algorithm='HS256')