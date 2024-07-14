package encrypt.SpIndex.vector.kernel;

import org.apache.commons.lang3.ArrayUtils;
import org.encrypt.SpIndex.lib.sm.sm3.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 序列加密与解密计算组件
 */
public class CryptoChangeAndReverse {

    public static Double[][] changeList(Double[][] doubleF, double[] z, double[] w) {

        byte[][] xbyte = new byte[doubleF.length][6];
        byte[][] ybyte = new byte[doubleF.length][6];

        for (int i = 0; i < doubleF.length; i++) {
            Double x = doubleF[i][0];
            Double y = doubleF[i][1];
            byte[] xn = Util.fileByte(Util.long2byte(Math.round((x + 180) * 100000000000L)), 6);
            byte[] yn = Util.fileByte(Util.long2byte(Math.round((y + 180) * 100000000000L)), 6);
            xbyte[i] = xn;
            ybyte[i] = yn;
        }
        Double[][] changeRes = new Double[doubleF.length][2];
        byte[] xtmp;
        byte[] ytmp;

        byte[] head = new byte[]{64, 89};
        for (int i = 0; i < doubleF.length; i++) {
            byte[] initx;
            byte[] inity;

            byte[] zn = Util.fileByte(Util.convertDoubleToByteArray(z[i]), 8);
            byte[] wn = Util.fileByte(Util.convertDoubleToByteArray(w[i]), 8);
            byte[] zl = Arrays.copyOfRange(zn, 0, 2);
            byte[] zr = Arrays.copyOfRange(zn, 2, 8);
            byte[] wl = Arrays.copyOfRange(wn, 0, 2);
            byte[] wr = Arrays.copyOfRange(wn, 2, 8);

            if (i == doubleF.length - 1) {
                initx = Arrays.copyOfRange(Util.fileByte(Util.convertDoubleToByteArray(z[0]), 8), 2, 8);
                inity = Arrays.copyOfRange(Util.fileByte(Util.convertDoubleToByteArray(w[0]), 8), 2, 8);
            } else {
                initx = xbyte[i + 1];
                inity = ybyte[i + 1];
            }

            xtmp = Util.XORByteArray(xbyte[i], Util.XORByteArray(byteArrayConcat(Arrays.copyOfRange(Util.XORByteArray(initx, zr), 2, 6), zl), zr));
            ytmp = Util.XORByteArray(ybyte[i], Util.XORByteArray(byteArrayConcat(Arrays.copyOfRange(Util.XORByteArray(inity, wr), 2, 6), wl), wr));

            byte[] xres = ArrayUtils.addAll(head, xtmp);
            byte[] yres = ArrayUtils.addAll(head, ytmp);
            changeRes[i][0] = Util.convertByteArrayToDouble(xres);
            changeRes[i][1] = Util.convertByteArrayToDouble(yres);
        }
        return changeRes;
    }

    public static Double[][] ichangeList(List<Double[]> doubleF, double[] z, double[] w) {
        byte[][] xbyte = new byte[doubleF.size()][6];
        byte[][] ybyte = new byte[doubleF.size()][6];

        for (int i = 0; i < doubleF.size(); i++) {
            xbyte[i] = Arrays.copyOfRange(Util.fileByte(Util.convertDoubleToByteArray(doubleF.get(i)[0]), 8), 2, 8);
            ybyte[i] = Arrays.copyOfRange(Util.fileByte(Util.convertDoubleToByteArray(doubleF.get(i)[1]), 8), 2, 8);
        }
        Double[][] changeRes = new Double[doubleF.size()][2];
        byte[] xtmp = new byte[0];
        byte[] ytmp = new byte[0];
        for (int i = doubleF.size() - 1; i >= 0; i--) {
            byte[] initx;
            byte[] inity;
            if (i == doubleF.size() - 1) {
                initx = Arrays.copyOfRange(Util.fileByte(Util.convertDoubleToByteArray(z[0]), 8), 2, 8);
                inity = Arrays.copyOfRange(Util.fileByte(Util.convertDoubleToByteArray(w[0]), 8), 2, 8);
            } else {
                initx = xtmp;
                inity = ytmp;
            }
            byte[] zn = Util.fileByte(Util.convertDoubleToByteArray(z[i]), 8);
            byte[] wn = Util.fileByte(Util.convertDoubleToByteArray(w[i]), 8);
            byte[] zl = Arrays.copyOfRange(zn, 0, 2);
            byte[] zr = Arrays.copyOfRange(zn, 2, 8);
            byte[] wl = Arrays.copyOfRange(wn, 0, 2);
            byte[] wr = Arrays.copyOfRange(wn, 2, 8);

            xtmp = Util.XORByteArray(Util.XORByteArray(xbyte[i], zr), byteArrayConcat(Arrays.copyOfRange(Util.XORByteArray(initx, zr), 2, 6), zl));
            ytmp = Util.XORByteArray(Util.XORByteArray(ybyte[i], wr), byteArrayConcat(Arrays.copyOfRange(Util.XORByteArray(inity, wr), 2, 6), wl));

            changeRes[i][0] = (double) (Util.byteArray2Long(Util.fileByte(xtmp, 8))) / 100000000000L - 180;
            changeRes[i][1] = (double) (Util.byteArray2Long(Util.fileByte(ytmp, 8))) / 100000000000L - 180;
        }
        return changeRes;
    }

    public static byte[] byteArrayConcat(byte[] a, byte[] b) {
        byte[] res = new byte[a.length + b.length];
        System.arraycopy(a, 0, res, 0, a.length);
        System.arraycopy(b, 0, res, a.length, a.length + b.length - a.length);
        return res;
    }

    public static Double[][] chaoticScrambling(double[] logRes, List<Double[]> d) {
        int sizeOfData = d.size();
        Double[][] logAndd1 = new Double[sizeOfData][3];
        for (int i = 0; i < sizeOfData; i++) {
            logAndd1[i][0] = logRes[i];
            logAndd1[i][1] = d.get(i)[0];
            logAndd1[i][2] = d.get(i)[1];
        }
        Arrays.sort(logAndd1, (o1, o2) -> checkVal(o1[0], o2[0]));
        Double[][] Res = new Double[logAndd1.length][2];
        for (int i = 0; i < logAndd1.length; i++) {
            Res[i][0] = logAndd1[i][1];
            Res[i][1] = logAndd1[i][2];
        }
        return Res;
    }

    public static Double[][] chaoticScramblingReverse(double[] logRes, Double[][] d) {
        int sizeOfData = d.length;
        Double[][] logAndd1 = new Double[sizeOfData][2];
        for (int i = 0; i < sizeOfData; i++) {
            logAndd1[i][0] = logRes[i];
            logAndd1[i][1] = (double) i;
        }
        Arrays.sort(logAndd1, (o1, o2) -> checkVal(o1[0], o2[0]));
        Double[][] logAndd2 = new Double[sizeOfData][3];
        for (int i = 0; i < sizeOfData; i++) {
            logAndd2[i][0] = logAndd1[i][1];
            logAndd2[i][1] = d[i][0];
            logAndd2[i][2] = d[i][1];
        }
        Arrays.sort(logAndd2, (o1, o2) -> checkVal(o1[0], o2[0]));

        Double[][] Res = new Double[logAndd2.length][2];
        for (int i = 0; i < logAndd2.length; i++) {
            Res[i][0] = logAndd2[i][1];
            Res[i][1] = logAndd2[i][2];
        }
        return Res;
    }

    private static int checkVal(double a, double b) {
        if (a - b >= 0) return 1;
        else return -1;
    }

    public static void main(String[] args) {

    }

}
