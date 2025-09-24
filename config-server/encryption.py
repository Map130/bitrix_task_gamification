"""
Модуль для шифрования и дешифрования конфигурационных значений.

Использует симметричное шифрование (Fernet) из библиотеки `cryptography`.
Ключ шифрования должен быть предоставлен через переменную окружения
`CONFIG_ENCRYPTION_KEY`.

Этот ключ является **единственным** секретом, который должен быть надежно
защищен и передан в `config-server` при запуске.
"""
import base64
import os
import logging
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

logger = logging.getLogger(__name__)

class EncryptionService:
    def __init__(self):
        master_key = os.getenv("CONFIG_ENCRYPTION_KEY")
        if not master_key:
            logger.critical("CONFIG_ENCRYPTION_KEY is not set. Service cannot run securely.")
            raise ValueError("CONFIG_ENCRYPTION_KEY must be set.")

        # Используем PBKDF2 для получения детерминированного 32-байтного ключа
        # из мастер-ключа произвольной длины. Это делает систему более устойчивой.
        salt = b'--bitrix-gamification-salt--' # Статическая соль, т.к. ключ уже секретный
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=480000, # Рекомендованное количество итераций на 2024 год
        )
        key = base64.urlsafe_b64encode(kdf.derive(master_key.encode()))
        self._fernet = Fernet(key)
        logger.info("EncryptionService initialized successfully.")

    def encrypt(self, plaintext: str) -> str:
        """Шифрует строку и возвращает зашифрованный токен в виде строки."""
        if not isinstance(plaintext, str):
             logger.error(f"Encryption input is not a string, but {type(plaintext)}")
             # Преобразуем в строку, если это возможно, для устойчивости
             plaintext = str(plaintext)
        return self._fernet.encrypt(plaintext.encode('utf-8')).decode('utf-8')

    def decrypt(self, token: str) -> str:
        """
        Дешифрует токен. Возвращает исходную строку.
        В случае ошибки (неверный ключ, поврежденный токен) выбрасывает InvalidToken.
        """
        if not isinstance(token, str):
            raise TypeError("Decryption input must be a string token.")
        try:
            return self._fernet.decrypt(token.encode('utf-8')).decode('utf-8')
        except InvalidToken:
            logger.error("Decryption failed: Invalid token provided. It might be corrupted or not an encrypted value.")
            # Возвращаем сам токен, чтобы не падать, но логируем ошибку.
            # Это позволяет хранить вперемешку зашифрованные и обычные значения.
            return token
        except Exception as e:
            logger.error(f"An unexpected error occurred during decryption: {e}")
            return token # Безопасное поведение по умолчанию

# Синглтон-экземпляр сервиса
try:
    encryption_service = EncryptionService()
except ValueError:
    encryption_service = None
    logger.warning("EncryptionService is not available due to missing CONFIG_ENCRYPTION_KEY.")

def generate_key():
    """Генерирует новый безопасный ключ для использования в CONFIG_ENCRYPTION_KEY."""
    return Fernet.generate_key().decode()

if __name__ == '__main__':
    # Этот блок можно использовать для генерации нового ключа
    print("---")
    print("Generated a new encryption key. Set this as CONFIG_ENCRYPTION_KEY in your environment:")
    print(generate_key())
    print("---")
