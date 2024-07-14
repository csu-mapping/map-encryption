package encrypt.SpIndex.vector.kernel;

import java.util.Arrays;

public class Hyperchaotic4D {
    private static final double a = 0.1;
    private static final double b = 0.1;
    private static final double c = 14;
    private static final double p = -20;
    private static final double q = -0.28;

    public Hyperchaotic4D() {
    }

    public static double[] arithmetic(double[] init) {
        double[] res = new double[4];
        res[0] = init[0] + 0.001 * (-init[1] - init[2] + init[3]);
        res[1] = init[1] + 0.001 * (init[0] + a * init[1] - init[2]);
        res[2] = init[2] + 0.001 * (b + init[2] * (init[0] - c) + p * init[0] * init[3]);
        res[3] = init[3] + 0.001 * (-init[0] * init[1] + q * init[3]);
        return res;
    }

    public static double[][] run(double[] initValue, int start, int len) {
//        System.out.println(Arrays.toString(initValue));
        double[][] calRes = new double[initValue.length][len];
        for (int i = 0; i < start; i++) {
            initValue = arithmetic(initValue);
        }
        for (int i = 0; i < len; i++) {
            initValue = arithmetic(initValue);
//            System.out.println(Arrays.toString(initValue));
            calRes[0][i] = initValue[0] * 100000 - Math.round(initValue[0] * 100000);
            calRes[1][i] = initValue[1] * 100000 - Math.round(initValue[1] * 100000);
            calRes[2][i] = initValue[2] * 100000 - Math.round(initValue[2] * 100000);
            calRes[3][i] = initValue[3] * 100000 - Math.round(initValue[3] * 100000);
        }
        return calRes;
    }


    public static void main(String[] args) {
    }
}
