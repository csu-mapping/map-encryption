package encrypt.SpIndex.vector.kernel;

import org.encrypt.SpIndex.lib.sm.sm3.Util;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Crypto4D {
    /**
     * 根据Key对待加密序列进行加解密计算
     *
     * @param keySM3 初始Key
     * @param d      待加密序列
     * @param type   加密解密方法选择
     * @return 加密结果序列
     */
    public static Double[][] cryptoMethod(byte[] keySM3, List<Double[]> d, int num, String type) {
//        System.out.println(Arrays.toString(keySM3));
        int e1 = Util.byteToInt(Arrays.copyOfRange(keySM3, 0, 4));
        int e2 = Util.byteToInt(Arrays.copyOfRange(keySM3, 4, 8));
        int e3 = Util.byteToInt(Arrays.copyOfRange(keySM3, 8, 12));
        int e4 = Util.byteToInt(Arrays.copyOfRange(keySM3, 12, 16));
        int e5 = Util.byteToInt(Arrays.copyOfRange(keySM3, 16, 20));
        int e6 = Util.byteToInt(Arrays.copyOfRange(keySM3, 20, 24));
        int e7 = Util.byteToInt(Arrays.copyOfRange(keySM3, 24, 28));
        int e8 = Util.byteToInt(Arrays.copyOfRange(keySM3, 28, 32));

        double ux = getDecimal((double) (e1 ^ e2 + e2 ^ e3 + e3 ^ e4) / d.size() / 100000000);
        double uy = getDecimal((double) (e3 ^ e4 + e4 ^ e5 + e5 ^ e6) / d.size() / 100000000);
        double uz = getDecimal((double) (e5 ^ e6 + e6 ^ e7 + e7 ^ e8) / d.size() / 100000000);
        double uw = getDecimal((double) (e7 ^ e8 + e8 ^ e1 + e1 ^ e2) / d.size() / 100000000);
        double[] init = new double[]{ux, uy, uz, uw};
        double[][] logBinary = Hyperchaotic4D.run(init, Math.floorMod(d.size() * num, 1024) + 1000, d.size());
        double[] x = logBinary[0];
        double[] y = logBinary[1];
        double[] z = logBinary[2];
        double[] w = logBinary[3];

        switch (type) {
            case "ENCRYPT":
                Double[][] scramblingRes = CryptoChangeAndReverse.chaoticScrambling(x, d);
                return CryptoChangeAndReverse.changeList(scramblingRes, z, w);
            case "DECRYPT":
                Double[][] changeRes = CryptoChangeAndReverse.ichangeList(d, z, w);
                return CryptoChangeAndReverse.chaoticScramblingReverse(x, changeRes);
            default:
                throw new IllegalArgumentException("Invalid encrypt type.");
        }
    }

    public static double getDecimal(double num) {
        return Math.abs(num - Math.round(num));
    }

    public static void main(String[] args) throws Exception {
        List<Double[]> doubleList = new ArrayList<>();
    }
}
