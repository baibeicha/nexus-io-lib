package com.github.baibeicha.nexus.io.crypto;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.security.SecureRandom;

public class AesGcmCipher {

    private static final String ALGO = "AES/GCM/NoPadding";
    private static final int TAG_LENGTH_BIT = 128; // 16 bytes auth tag
    private static final int IV_LENGTH_BYTE = 12;  // 12 bytes IV (стандарт для GCM)

    public static byte[] generateIv() {
        byte[] iv = new byte[IV_LENGTH_BYTE];
        new SecureRandom().nextBytes(iv);
        return iv;
    }

    public static Cipher getEncryptCipher(SecretKey key, byte[] iv) throws Exception {
        Cipher cipher = Cipher.getInstance(ALGO);
        cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_LENGTH_BIT, iv));
        return cipher;
    }

    public static Cipher getDecryptCipher(SecretKey key, byte[] iv) throws Exception {
        Cipher cipher = Cipher.getInstance(ALGO);
        cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_LENGTH_BIT, iv));
        return cipher;
    }
}
