from os.path import join
from pathlib import Path

from cryptography.fernet import Fernet

BIN_FILE = join(str(Path.home()), 'mysql_bytes.bin')


class Decrypt:

    @classmethod
    def initialize(cls, key):
        cipher_suite = Fernet(key)
        with open(BIN_FILE, 'rb') as file_object:
            for line in file_object:
                pwd_encrypted = line
        pwd_decrypted = cipher_suite.decrypt(pwd_encrypted)
        pwd_plain_text = bytes(pwd_decrypted).decode("utf-8")
        return pwd_plain_text
