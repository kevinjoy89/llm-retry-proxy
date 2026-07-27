import json
import unittest

from retry_proxy.secrets_crypto import (SENSITIVE_FIELDS, decrypt_session,
                                        derive_key, encrypt_session)


class SecretsCryptoTests(unittest.TestCase):
    def test_encrypt_decrypt_roundtrip_restores_original_session(self):
        key = derive_key("master-secret")
        session = {
            "email": "user@example.com", "username": "user",
            "access_token": "tok-secret", "password": "p@ss",
            "refresh_token": "rt-xyz", "cookies": {"session": "c1"},
        }
        sealed = encrypt_session(session, SENSITIVE_FIELDS, key)
        self.assertTrue(sealed["__encrypted__"])
        self.assertEqual(sealed["v"], 1)

        restored = decrypt_session(sealed, key)
        self.assertEqual(restored, session)

    def test_no_secret_returns_plaintext_unchanged(self):
        self.assertIsNone(derive_key(""))
        key = derive_key("")
        session = {"access_token": "tok", "email": "u@x.com"}
        # key is None -> encrypt_session returns session unchanged
        self.assertIs(encrypt_session(session, SENSITIVE_FIELDS, key), session)
        self.assertIs(decrypt_session(session, key), session)

    def test_decrypt_legacy_plaintext_session_passes_through(self):
        key = derive_key("master-secret")
        legacy = {"email": "u@x.com", "access_token": "legacy-tok",
                  "password": "legacy-pw"}
        # Old plaintext files have no __encrypted__ marker; should pass through
        # unchanged so they get re-encrypted on next save (auto-migration).
        self.assertEqual(decrypt_session(legacy, key), legacy)

    def test_wrong_key_raises_value_error(self):
        sealed = encrypt_session(
            {"access_token": "tok", "password": "pw"}, SENSITIVE_FIELDS,
            derive_key("right-key"),
        )
        with self.assertRaises(ValueError):
            decrypt_session(sealed, derive_key("wrong-key"))

    def test_only_sensitive_fields_are_encrypted(self):
        key = derive_key("master-secret")
        session = {
            "email": "visible@example.com", "username": "visible",
            "access_token": "hidden-tok", "password": "hidden-pw",
            "refresh_token": "hidden-rt", "cookies": {"s": "hidden"},
        }
        sealed = encrypt_session(session, SENSITIVE_FIELDS, key)

        # Non-sensitive fields remain plaintext and inspectable
        self.assertEqual(sealed["email"], "visible@example.com")
        self.assertEqual(sealed["username"], "visible")

        # Sensitive fields are absent from the plaintext layer
        for field in SENSITIVE_FIELDS:
            self.assertNotIn(field, sealed)

        # The ciphertext does not leak the secret values
        blob = json.dumps(sealed, ensure_ascii=False)
        self.assertNotIn("hidden-tok", blob)
        self.assertNotIn("hidden-pw", blob)
        self.assertNotIn("hidden-rt", blob)

    def test_session_without_sensitive_fields_is_not_sealed(self):
        key = derive_key("master-secret")
        session = {"email": "u@x.com", "username": "u"}
        sealed = encrypt_session(session, SENSITIVE_FIELDS, key)
        # No sensitive fields -> no encryption overhead, returned as-is
        self.assertNotIn("__encrypted__", sealed)
        self.assertEqual(sealed, session)

    def test_decrypt_corrupt_data_raises_value_error(self):
        key = derive_key("master-secret")
        sealed = encrypt_session(
            {"password": "pw"}, SENSITIVE_FIELDS, key,
        )
        sealed["data"] = "garbage-not-a-valid-token"
        with self.assertRaises(ValueError):
            decrypt_session(sealed, key)

    def test_decrypt_encrypted_without_key_raises_value_error(self):
        sealed = encrypt_session(
            {"password": "pw"}, SENSITIVE_FIELDS, derive_key("has-key"),
        )
        # No key available (None) but data is encrypted -> cannot recover
        with self.assertRaises(ValueError):
            decrypt_session(sealed, None)


if __name__ == "__main__":
    unittest.main()
