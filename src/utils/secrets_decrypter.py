"""
Utilities for decrypting OneDrive OAuth secrets stored in master configuration.

Decrypts values encrypted by cv-service using AES-128-CBC with HMAC-SHA256.
Format: "enc:" + base64url(Version || Timestamp || IV || Ciphertext || HMAC)
Encryption key from env var: ONEDRIVE_MASTER_FERNET_KEY (defaults to dev key if not set)

If a value does not start with "enc:", it is returned as-is (allows plaintext in dev).
"""

from __future__ import annotations

import os
import base64
import hashlib
import logging
from typing import Optional
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import hashes, hmac
from cryptography.hazmat.backends import default_backend

logger = logging.getLogger(__name__)


class SecretDecryptError(RuntimeError):
    pass


def decrypt_maybe(value: Optional[str]) -> Optional[str]:
    """
    Decrypt a value if it's encrypted, otherwise return as-is.
    
    Args:
        value: The value to decrypt (may be plaintext or encrypted with "enc:" prefix)
        
    Returns:
        Decrypted plaintext value, or original value if not encrypted
        
    Raises:
        SecretDecryptError: If decryption fails
    """
    if value is None:
        return None

    if not isinstance(value, str):
        return str(value)

    # If not encrypted, return as-is
    if not value.startswith("enc:"):
        return value

    # Get encryption key from environment
    key_string = os.getenv("ONEDRIVE_MASTER_FERNET_KEY")
    if not key_string:
        # Use default key if not set (matches cv-service behavior)
        logger.warning(
            "ONEDRIVE_MASTER_FERNET_KEY not set, using default key (NOT SECURE FOR PRODUCTION)"
        )
        key_string = "default-fernet-key-min-32-chars-for-dev-only"

    try:
        # Derive 32-byte key from string (SHA256 hash)
        key_bytes = hashlib.sha256(key_string.encode("utf-8")).digest()[:32]
        aes_key = key_bytes[:16]  # First 16 bytes for AES
        hmac_key = key_bytes[16:32]  # Last 16 bytes for HMAC

        # Decode base64url token (remove "enc:" prefix and convert to base64)
        token_base64url = value[4:]
        token_base64 = token_base64url.replace("-", "+").replace("_", "/")
        padding = len(token_base64) % 4
        if padding:
            token_base64 += "=" * (4 - padding)
        
        token_bytes = base64.b64decode(token_base64)

        # Parse token: Version(1) || Timestamp(8) || IV(16) || Ciphertext || HMAC(32)
        min_size = 1 + 8 + 16 + 32
        if len(token_bytes) < min_size:
            raise ValueError(f"Token too short: {len(token_bytes)} bytes")

        iv = token_bytes[9:25]
        hmac_received = token_bytes[-32:]
        ciphertext = token_bytes[25:-32]

        # Verify HMAC
        payload = token_bytes[:-32]
        h = hmac.HMAC(hmac_key, hashes.SHA256(), backend=default_backend())
        h.update(payload)
        h.verify(hmac_received)

        # Decrypt with AES-128-CBC
        cipher = Cipher(algorithms.AES(aes_key), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        decrypted = decryptor.update(ciphertext) + decryptor.finalize()

        # Remove PKCS7 padding
        if len(decrypted) > 0:
            padding_length = decrypted[-1]
            if 1 <= padding_length <= 16:
                padding_bytes = decrypted[-padding_length:]
                if all(b == padding_length for b in padding_bytes):
                    decrypted = decrypted[:-padding_length]

        # Decode to UTF-8 and clean up control characters
        result = decrypted.decode("utf-8")
        result = result.rstrip('\x00\x01\x02\x03\x04\x05\x06\x07\x08\x0b\x0c\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f')
        return result

    except ValueError as e:
        # Re-raise ValueError as SecretDecryptError with more context
        raise SecretDecryptError(f"Failed to decrypt secret: {str(e)}") from e
    except Exception as e:
        logger.error(f"Unexpected error during decryption: {str(e)}", exc_info=True)
        raise SecretDecryptError(f"Failed to decrypt secret: {str(e)}") from e

