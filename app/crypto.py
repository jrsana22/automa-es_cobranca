"""Criptografia de senhas — separado para evitar import circular."""

import base64
from cryptography.fernet import Fernet

from app.config import settings

_fernet = None


def get_fernet():
    global _fernet
    if _fernet is None:
        raw_key = settings.ENCRYPTION_KEY.encode('utf-8')
        # Fernet precisa de 32 bytes url-safe base64-encoded
        # Se a chave for o padrão de dev, gerar uma determinística
        if raw_key == b'dev-encryption-key-32bytes!!':
            # Derivar uma chave Fernet válida a partir da dev key
            import hashlib
            derived = hashlib.sha256(raw_key).digest()
            key = base64.urlsafe_b64encode(derived)
        else:
            # Para chaves de produção, usar base64 direto se já for válido
            try:
                key = base64.urlsafe_b64decode(raw_key + b'=' * (4 - len(raw_key) % 4))
                key = base64.urlsafe_b64encode(key)
            except Exception:
                import hashlib
                derived = hashlib.sha256(raw_key).digest()
                key = base64.urlsafe_b64encode(derived)
        _fernet = Fernet(key)
    return _fernet


def encrypt_password(password: str) -> str:
    f = get_fernet()
    return f.encrypt(password.encode()).decode()


def decrypt_password(encrypted: str) -> str:
    f = get_fernet()
    return f.decrypt(encrypted.encode()).decode()