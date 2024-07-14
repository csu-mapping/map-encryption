package encrypt.SpIndex.lib.sm.sm3;

import org.apache.commons.lang3.ArrayUtils;
import org.bouncycastle.crypto.digests.SM3Digest;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.pqc.math.linearalgebra.ByteUtils;
import org.encrypt.SpIndex.lib.sm.sm4.Util;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.util.Arrays;

public class SM3 {

    private static final String ENCODING = "UTF-8";

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    /**
     * SM3加密
     *
     * @param paramStr 待加密字符串
     * @return 返回加密后，固定长度=32的16进制字符串
     */
    public static String encrypt(String paramStr) {
        //将返回的hash值转换为16进制字符串
        String resultHexString = "";
        try {
            //将字符串转换成byte数组
            byte[] srcData = paramStr.getBytes(ENCODING);
            byte[] resultHash = hash(srcData);
            //将返回的hash值转换成16进制字符串
            resultHexString = ByteUtils.toHexString(resultHash);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return resultHexString;
    }

    /**
     * SM3加密
     *
     * @param paramStr 待加密比特字符
     * @return 返回加密后，固定长度=32的16进制字符串
     */
    public static String encrypt(byte[] paramStr) {
        //将返回的hash值转换为16进制字符串
        String resultHexString = "";
        try {
            //将字符串转换成byte数组
            byte[] resultHash = hash(paramStr);
            //将返回的hash值转换成16进制字符串
            resultHexString = ByteUtils.toHexString(resultHash);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultHexString;
    }

    /**
     * SM3加密
     *
     * @param paramStr 待加密字符串
     * @return 返回加密后
     */
    public static byte[] encryptStringToByte(String paramStr) {
        //将返回的hash值转换为16进制字符串
        String resultHexString = "";
        byte[] resultHash = null;
        try {
            //将字符串转换成byte数组
            byte[] srcData = paramStr.getBytes(ENCODING);
            resultHash = hash(srcData);
            //将返回的hash值转换成16进制字符串
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return resultHash;
    }

    /**
     * 返回长度为32的byte数组
     * 生成对应的hash值
     *
     * @param srcData
     * @return
     */
    public static byte[] hash(byte[] srcData) {
        SM3Digest digest = new SM3Digest();
        digest.update(srcData, 0, srcData.length);
        byte[] hash = new byte[digest.getDigestSize()];
        digest.doFinal(hash, 0);
        return hash;
    }

    /**
     * 通过指定密钥进行加密
     *
     * @param key     密钥
     * @param srcData 被加密的byte数组
     * @return
     */
    public static byte[] hmac(byte[] key, byte[] srcData) {
        KeyParameter keyParameter = new KeyParameter(key);
        SM3Digest digest = new SM3Digest();
        HMac mac = new HMac(digest);
        mac.init(keyParameter);
        mac.update(srcData, 0, srcData.length);
        byte[] result = new byte[mac.getMacSize()];
        mac.doFinal(result, 0);
        return result;
    }

    public static byte[] derivedKey(byte[] key, byte[] saltString, int c, int n) {
        byte[] paramInit = ArrayUtils.addAll(saltString, Util.int2byte(n));
        byte[] U = hmac(key, paramInit);
        byte[] result = new byte[U.length];
        for (int j = 0; j < c - 1; j++) {
            byte[] tt = U.clone();
            U = hmac(key, U);
            result = Util.XORByteArray(tt, U);
        }
        return result;
    }

    /**
     * 判断数据源与加密数据是否一致，通过验证原数组和生成是hash数组是否为同一数组，验证二者是否为同一数据
     *
     * @param srcStr
     * @param sm3HexString
     * @return
     */
    public static boolean vertify(String srcStr, String sm3HexString) {
        boolean flag = false;
        try {
            byte[] srcData = srcStr.getBytes(ENCODING);
            byte[] sm3Hash = ByteUtils.fromHexString(sm3HexString);
            byte[] newHash = hash(srcData);
            if (Arrays.equals(newHash, sm3Hash)) {
                flag = true;
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return flag;
    }

    public static void main(String[] args) {
    }
}
