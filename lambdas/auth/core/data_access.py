from contextlib import contextmanager
from .db import get_db_connection

@contextmanager
def get_cursor():
  connection = get_db_connection()
  cursor = connection.cursor()
  
  try:
    yield cursor
    connection.commit()
  except Exception as e:
    connection.rollback()
    raise e
  finally:
    cursor.close()
    
# 사용자 생성
def create_user(email: str, hashed_password: str, nickname: str) -> str:
  with get_cursor() as cur:
    cur.execute(
      "INSERT INTO users (email, hashed_password, nickname) VALUES (%s, %s, %s) RETURNING id",
      (email, hashed_password, nickname)
    )
    user_id = cur.fetchone()
    return str(user_id[0])  # 튜플의 첫 번째 요소만 반환
  
# user_id로 사용자 조회 (전체 정보)
def get_user_by_user_id(user_id: str):
  with get_cursor() as cur:
    cur.execute(
      "SELECT id, email, hashed_password, nickname, is_active FROM users WHERE id = %s",
      (user_id,)
    )
    user = cur.fetchone()
    if user:
      return {
        "id": str(user[0]),
        "email": user[1],
        "hashed_password": user[2],
        "nickname": user[3],
        "is_active": user[4],
      }
    return None

# email로 사용자 조회 (전체 정보)
def get_user_by_email(email: str):
  with get_cursor() as cur:
    cur.execute(
      "SELECT id, email, hashed_password, nickname, is_active FROM users WHERE email = %s",
      (email,)
    )
    user = cur.fetchone()
    if user:
      return {
        "id": str(user[0]),
        "email": user[1],
        "hashed_password": user[2],
        "nickname": user[3],
        "is_active": user[4],
      }
    return None
  
def activate_user(user_id: str):
  with get_cursor() as cur:
    cur.execute(
      "UPDATE users SET is_active = TRUE WHERE id = %s",
      (user_id,)
    )
    # UPDATE 쿼리는 결과를 반환하지 않으므로 fetchone() 제거
    return user_id
  
# 리프레시 토큰 저장
def store_refresh_token(user_id: str, token_hash: str):
  with get_cursor() as cur:
    cur.execute(
      "INSERT INTO refresh_tokens (user_id, token_hash, expires_at) VALUES (%s, %s, %s)",
      (user_id, token_hash, "2025-12-31 23:59:59")
    )

# 리프레시 토큰 무효화
def revoke_refresh_token(token_hash: str):
  with get_cursor() as cur:
    cur.execute(
      "UPDATE refresh_tokens SET is_revoked = TRUE WHERE token_hash = %s",
      (token_hash,)
    )

# 사용자 삭제 (CASCADE로 관련 데이터도 함께 삭제)
def delete_user(user_id: str):
  with get_cursor() as cur:
    cur.execute(
      "DELETE FROM users WHERE id = %s",
      (user_id,)
    )

# 리프레시 토큰 해시로 조회
def get_refresh_token_by_hash(token_hash: str):
  with get_cursor() as cur:
    cur.execute(
      "SELECT user_id, token_hash, expires_at, is_revoked FROM refresh_tokens WHERE token_hash = %s",
      (token_hash,)
    )
    token = cur.fetchone()
    if token:
      return {
        "user_id": str(token[0]),
        "token_hash": token[1],
        "expires_at": token[2],
        "is_revoked": token[3]
      }
    return None

# 닉네임 업데이트
def update_user_nickname(user_id: str, nickname: str):
  with get_cursor() as cur:
    cur.execute(
      "UPDATE users SET nickname = %s WHERE id = %s",
      (nickname, user_id)
    )

# 비밀번호 업데이트
def update_user_password(user_id: str, hashed_password: str):
  with get_cursor() as cur:
    cur.execute(
      "UPDATE users SET hashed_password = %s WHERE id = %s",
      (hashed_password, user_id)
    )

# 이메일로 사용자 조회 (비밀번호 찾기용 - 최소 정보만)
def get_user_by_email_for_reset(email: str):
  with get_cursor() as cur:
    cur.execute(
      "SELECT id, email, is_active FROM users WHERE email = %s",
      (email,)
    )
    user = cur.fetchone()
    if user:
      return {
        "id": str(user[0]),
        "email": user[1],
        "is_active": user[2]
      }
    return None

# 만료되거나 무효화된 리프레시 토큰 삭제
def cleanup_expired_tokens():
  with get_cursor() as cur:
    cur.execute(
      "DELETE FROM refresh_tokens WHERE expires_at < NOW() OR is_revoked = TRUE"
    )

# 사용자의 히스토리 조회 (runs와 reports JOIN)
def get_user_history(user_id: str):
  with get_cursor() as cur:
    cur.execute(
      """
      SELECT ru.id AS run_id, r.s3_key, r.title, r.summary_line
      FROM runs ru
      INNER JOIN reports r ON ru.id = r.run_id
      WHERE ru.user_id = %s
      ORDER BY r.created_at DESC
      """,
      (user_id,)
    )
    rows = cur.fetchall()
    return [
      {
        "run_id": str(row[0]),
        "s3_key": row[1],
        "title": row[2],
        "summary_line": row[3]
      }
      for row in rows
    ]

# 사용자의 히스토리 삭제 (run_id로 runs 삭제, CASCADE로 reports도 함께 삭제)
def delete_user_history(user_id: str, run_id: str):
  with get_cursor() as cur:
    # 먼저 해당 run_id가 해당 user_id에 속하는지 확인
    cur.execute(
      "SELECT id FROM runs WHERE id = %s AND user_id = %s",
      (run_id, user_id)
    )
    run = cur.fetchone()
    
    if not run:
      return False
    
    # runs 삭제 (CASCADE로 reports도 함께 삭제됨)
    cur.execute(
      "DELETE FROM runs WHERE id = %s AND user_id = %s",
      (run_id, user_id)
    )
    
    return True