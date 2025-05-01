import bcrypt
from jose import JWTError, jwt
from datetime import datetime, timedelta

SECRET_KEY = "maclesupersecrete"
ALGORITHM = "HS256"

def hash_password(password: str) -> str:

    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode()

def verify_password(password: str, hashed: str) -> bool:

    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode())

def create_token(data: dict, expires_delta: timedelta = timedelta(minutes=30)):

    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def decode_token(token: str):

    return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])