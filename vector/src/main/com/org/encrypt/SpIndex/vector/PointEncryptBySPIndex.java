package encrypt.SpIndex.vector;

import com.github.davidmoten.rtree.*;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.geometry.Point;
import com.github.davidmoten.rtree.geometry.Rectangle;
import org.apache.log4j.Logger;
import org.encrypt.SpIndex.db.PostgisDataStore;
import encrypt.SpIndex.lib.sm.sm3.SM3;
import encrypt.SpIndex.vector.kernel.Crypto4D;
import org.encrypt.SpIndex.vector.util.*;
import org.geotools.data.DataStore;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

import static com.github.davidmoten.rtree.geometry.Geometries.rectangle;
import static org.encrypt.SpIndex.db.PostgisDataStore.saveRTree;

public class PointEncryptBySPIndex {
    public static Logger log = Logger.getLogger(Class.class);

    private static class Tasker implements Callable<ThreeTuple<Integer, List, List<SimpleFeature>>> {
        private Leaf childLeaf;
        private HashMap<String, SimpleFeature> shpMap;
        private String saltString;
        private String password;

        public Tasker(Leaf childLeaf, HashMap<String, SimpleFeature> shpMap, String saltString, String password) {
            this.childLeaf = childLeaf;
            this.shpMap = shpMap;
            this.saltString = saltString;
            this.password = password;
        }

        @Override
        public ThreeTuple<Integer, List, List<SimpleFeature>> call() {
            List<Entry> leafEntry = childLeaf.entries();
            List<SimpleFeature> shpFeatures = new ArrayList<>();
            List<Double[]> pointList = new ArrayList<>();
            for (Entry leafRec : leafEntry) {
                Point shpRec = (Point) leafRec.geometry();
                SimpleFeature tmpFeature = shpMap.get(shpRec.toString());
                try {
//                    System.out.println(tmpFeature.getAttribute("geom"));
                    org.locationtech.jts.geom.Point tmpLine = (org.locationtech.jts.geom.Point) tmpFeature.getAttribute("geom");
                    Double[] corDouble = new Double[]{tmpLine.getX(), tmpLine.getY()};
                    pointList.add(corDouble);
                } catch (Exception e) {
                    log.info(e);
                }
            }
            byte[] key = password.getBytes(StandardCharsets.UTF_8);
            byte[] salt = Util.XORByteArray(SM3.encryptStringToByte(saltString), SM3.encryptStringToByte(String.valueOf(20 + 400)));
            int c = leafEntry.size();
            int n = 1;
            byte[] keySM3 = SM3.derivedKey(key, salt, c, n);
            Double[][] ResList = Crypto4D.cryptoMethod(keySM3, pointList, leafEntry.size(), "ENCRYPT");

            List<Integer> id = new ArrayList<>();
            for (int i = 0; i < ResList.length; i++) {
                Rectangle shpRec = (Rectangle) leafEntry.get(i).geometry();
                SimpleFeature tmpFeature = SimpleFeatureBuilder.copy(shpMap.get(shpRec.toString()));
                Coordinate tmpCor = new Coordinate(ResList[i][0], ResList[i][1]);

                org.locationtech.jts.geom.Point geoPoint = new GeometryFactory().createPoint(tmpCor);
                tmpFeature.setAttribute("geom", geoPoint);
                shpFeatures.add(tmpFeature);
                id.add((Integer) leafEntry.get(i).value());
            }
            return new ThreeTuple<>(pointList.size(), id, shpFeatures);
        }

    }

    public static boolean encryptMethodParallel(String tableName, String pubKeyFile, String outputPath, HashMap<String, List<Integer>> rtreeIndexLeaf) {
        try {
            log.info("*************" + tableName + "*************");
            log.info("*************" + Thread.currentThread().getStackTrace()[1].getClassName() + "*************");
            //读取公钥数据
            BufferedReader in = new BufferedReader(new FileReader(pubKeyFile));
            String pubKey = in.readLine();

            //读取数据
            long enTime00 = System.currentTimeMillis();

            HashMap<String, SimpleFeature> shpMap = new HashMap<>();
            List<Entry<Integer, Geometry>> entryList = new ArrayList<>();
//            IO.readPointShapefile(shapeDS, shpMap, entryList);

            DataStore dataStore = PostgisDataStore.getDataStore();
            Integer count = PostgisDataStore.readGeomFromDB(dataStore, tableName, shpMap, entryList);
            System.out.println("读取数据条数：" + count);

            long enTime01 = System.currentTimeMillis();
            log.info("point文件读取时间：" + (enTime01 - enTime00));

//          构建R树
            int maxChildren = 200;
            RTree<Integer, Geometry> tree = RTree.star().maxChildren(maxChildren).create(entryList);
            //保存rtree图片
//            tree.visualize(1000, 1000).save("data/point_rtree.png");
            Node<Integer, Geometry> root = tree.root().get();
            //遍历得到叶子节点
            List<Leaf> childrenList = Tools.traverseLeafPostOrder(root);
            log.info("叶子结点个数：" + childrenList.size());

            long enTime02 = System.currentTimeMillis();
            log.info("索引构建时间：" + (enTime02 - enTime01));

            String saltString = Tools.generateRandom(16);
            String password = Tools.generateRandom(16);

            List<Future<ThreeTuple<Integer, List, List<SimpleFeature>>>> futures = new ArrayList<>();

            ExecutorService threadPool = Executors.newFixedThreadPool(16);
            Future<ThreeTuple<Integer, List, List<SimpleFeature>>> res = null;
            List<String> keyList = new ArrayList<>();
            for (Leaf childLeaf : childrenList) {
                res = threadPool.submit(new Tasker(childLeaf, shpMap, saltString, password));
                futures.add(res);
                Rectangle leafRecBon = (Rectangle) childLeaf.geometry();
                keyList.add(leafRecBon.toString());
            }
            threadPool.shutdown();
//            while (!threadPool.awaitTermination(1, TimeUnit.MICROSECONDS)) {
////                System.out.println("线程池未关闭");
//            }

            long enTime03 = System.currentTimeMillis();
            log.info("point加密时间：" + (enTime03 - enTime02));
//
            saveRTree(outputPath + "Point", tree);
            String keyMerge = password + saltString;
            String outputFile = outputPath + "/" + tableName + "_encrypt.key";
            dataStore = PostgisDataStore.getDataStore();
//            HashMap<String, List<Integer>> rtreeIndexLeaf = new HashMap<>();
//            + Tools.generateRandom(1)
            PostgisDataStore.writeGeomToDB(dataStore, tableName + "_encrypt", futures, org.locationtech.jts.geom.Point.class, keyMerge, pubKey, outputFile, count, rtreeIndexLeaf, keyList);

            long enTime04 = System.currentTimeMillis();
            log.info("写入时间：" + (enTime04 - enTime03));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private static boolean encryptMethodSingle(String tableName, String pubKeyFile, String outputPath) {
        try {
            log.info("*************" + tableName + "*************");
            log.info("*************" + Thread.currentThread().getStackTrace()[1].getClassName() + "*************");
            log.info("*************" + Thread.currentThread().getStackTrace()[1].getMethodName() + "*************");

            //读取公钥数据
            BufferedReader in = new BufferedReader(new FileReader(pubKeyFile));
            String pubKey = in.readLine();

            //读取数据
            long enTime00 = System.currentTimeMillis();

            HashMap<String, SimpleFeature> shpMap = new HashMap<>();
            List<Entry<Integer, Geometry>> entryList = new ArrayList<>();
//            IO.readPointShapefile(shapeDS, shpMap, entryList);

            DataStore dataStore = PostgisDataStore.getDataStore();
            Integer count = PostgisDataStore.readGeomFromDB(dataStore, tableName, shpMap, entryList);
            System.out.println("读取数据条数：" + count);

            long enTime01 = System.currentTimeMillis();
            log.info("point文件读取时间：" + (enTime01 - enTime00));

//          构建R树
//            全国点2000最优
            int maxChildren = 200;
            RTree<Integer, Geometry> tree = RTree.star().maxChildren(maxChildren).create(entryList);
            //保存rtree图片
//            tree.visualize(1000, 1000).save("data/vector/point/rtree.png");
            Node<Integer, Geometry> root = tree.root().get();
            //遍历得到叶子节点
            List<Leaf> childrenList = Tools.traverseLeafPostOrder(root);
            log.info("叶子结点个数：" + childrenList.size());

            long enTime02 = System.currentTimeMillis();
            log.info("索引构建时间：" + (enTime02 - enTime01));

            String saltString = Tools.generateRandom(16);
            String password = Tools.generateRandom(16);

            List futures = new ArrayList<>();
            List<String> keyList = new ArrayList<>();
            for (Leaf childLeaf : childrenList) {
                List res = new ArrayList<>();
                List<Entry> leafEntry = childLeaf.entries();
//            Rectangle leafRecBon = (Rectangle) childLeaf.geometry();
                List<SimpleFeature> shpFeatures = new ArrayList<>();
                List<Double[]> pointList = new ArrayList<>();
                for (Entry leafRec : leafEntry) {
                    Point shpRec = (Point) leafRec.geometry();
                    SimpleFeature tmpFeature = shpMap.get(shpRec.toString());
                    try {
                        org.locationtech.jts.geom.Point tmpLine = (org.locationtech.jts.geom.Point) tmpFeature.getAttribute("geom");
                        Double[] corDouble = new Double[]{tmpLine.getX(), tmpLine.getY()};
                        pointList.add(corDouble);
                    } catch (Exception e) {
                        log.info(e);
                    }
                }
                byte[] key = password.getBytes(StandardCharsets.UTF_8);
                byte[] salt = Util.XORByteArray(SM3.encryptStringToByte(saltString), SM3.encryptStringToByte(String.valueOf(20 + 400)));
                int c = leafEntry.size();
                int n = 1;
                byte[] keySM3 = SM3.derivedKey(key, salt, c, n);
                Double[][] ResList = Crypto4D.cryptoMethod(keySM3, pointList, leafEntry.size(), "ENCRYPT");

                for (int i = 0; i < ResList.length; i++) {
                    Rectangle shpRec = (Rectangle) leafEntry.get(i).geometry();
                    SimpleFeature tmpFeature = SimpleFeatureBuilder.copy(shpMap.get(shpRec.toString()));
                    Coordinate tmpCor = new Coordinate(ResList[i][0], ResList[i][1]);

                    org.locationtech.jts.geom.Point geoPoint = new GeometryFactory().createPoint(tmpCor);
                    tmpFeature.setAttribute("geom", geoPoint);

                    shpFeatures.add(tmpFeature);
                }
                res.add(pointList.size());
                res.add(n);
                res.add(shpFeatures);
                futures.add(res);
                Rectangle leafRecBon = (Rectangle) childLeaf.geometry();
                keyList.add(leafRecBon.toString());
            }

            long enTime03 = System.currentTimeMillis();
            log.info("point加密时间：" + (enTime03 - enTime02));

            long enTime04 = System.currentTimeMillis();
            log.info("写入时间：" + (enTime04 - enTime03));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static List<List<Double[]>> searchMethod(String indexFile, String tableName, Rectangle searchRec, HashMap<String, List<Integer>> rtreeIndexLeaf) {
        try {
            //读取数据
            Serializer<Integer, Geometry> serializer = Serializers.flatBuffers().javaIo();
            File file = new File(indexFile);
            InputStream is = new FileInputStream(file);
            RTree<Integer, Geometry> tree = serializer.read(is, file.length(), InternalStructure.DEFAULT);

//            FileInputStream fis = new FileInputStream(dataFile);
//            ObjectInputStream ois = new ObjectInputStream(fis);
//            HashMap<String, List<List<Object>>> dataStoreNew = (HashMap<String, List<List<Object>>>) ois.readObject();

            HashMap<String, SimpleFeature> shpMap = new HashMap<>();
            List<Entry<Integer, Geometry>> entryList = new ArrayList<>();
//            IO.readPointShapefile(shapeDS, shpMap, entryList);

            DataStore dataStore = PostgisDataStore.getDataStore();
            PostgisDataStore.readGeomFromDB(dataStore, tableName, shpMap, entryList);

            long enTime0 = System.currentTimeMillis();
            List<Leaf> searchLeafRes = Tools.intersection(searchRec, tree.root().get());
            System.out.println("查找叶子结点个数：" + searchLeafRes.size());
            List<List<Double[]>> pointAttrList = new ArrayList<>();
            for (Leaf rec : searchLeafRes) {
                String searchRecString = rec.geometry().mbr().toString();
                List<Integer> singleResList = rtreeIndexLeaf.get(searchRecString);
//                System.out.println(singleResList);
                List<Double[]> tmpList = new ArrayList<>();
                for (Integer list : singleResList) {
                    try {
//                        System.out.println(entryList.get(list).geometry());
//                        System.out.println(shpMap.get(entryList.get(list).geometry().toString()));
                        org.locationtech.jts.geom.Point tmp = (org.locationtech.jts.geom.Point) shpMap.get(entryList.get(list).geometry().toString()).getAttribute("geom");
                        tmpList.add(new Double[]{tmp.getX(), tmp.getY()});
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                pointAttrList.add(tmpList);
            }
            long enTime1 = System.currentTimeMillis();
            log.info("point搜索时间：" + (enTime1 - enTime0));

            return pointAttrList;

//            Map<String, Class<?>> schema = (Map<String, Class<?>>) dataStoreNew.get("schema").get(0).get(0);
//            pointAttrList.add(Collections.singletonList(schema));
//
//            long enTime2 = System.currentTimeMillis();
//            log.info("point加密时间：" + (enTime2 - enTime1));

//            FileOutputStream fos = new FileOutputStream(outputIndex);
//            ObjectOutputStream oos1 = new ObjectOutputStream(fos);
//            oos1.writeObject(pointAttrList);
//            oos1.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static boolean decryptMethod(List<List<Double[]>> pointAttrList, String saveTableName, String keyFile, String priKeyFile, String outputPath) {
        try {
            //读取私钥数据
            BufferedReader in = new BufferedReader(new FileReader(priKeyFile));
            String priKey = in.readLine();
            //读取数据
            long enTime00 = System.currentTimeMillis();

            HashMap<String, SimpleFeature> shpMap = new HashMap<>();
            List<Entry<Integer, Geometry>> entryList = new ArrayList<>();

            DataStore dataStore = PostgisDataStore.getDataStore();
            SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
            builder.setName(saveTableName);
            builder.setCRS(null); // <- Coordinate reference system; null means default
            builder.add("id", Integer.class);
            builder.add("geom", org.locationtech.jts.geom.Point.class);
            SimpleFeatureType schema = builder.buildFeatureType();
            assert dataStore != null;
            // Check if the schema exists
            String typeName = schema.getTypeName();
            if (Arrays.asList(dataStore.getTypeNames()).contains(typeName)) {
                // If the schema exists, remove it
                dataStore.removeSchema(typeName);
            }
            dataStore.createSchema(schema);

            long enTime01 = System.currentTimeMillis();
            log.info("point文件读取时间：" + (enTime01 - enTime00));

            FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriterAppend(schema.getTypeName(), Transaction.AUTO_COMMIT);

            int last = 0;
            for (List<Double[]> pointList : pointAttrList) {
                last += pointList.size();
                String keyMerge = Tools.generateRandom(16);
                String keyString = keyMerge.substring(0, 16);
                byte[] key = keyString.getBytes(StandardCharsets.UTF_8);
                byte[] salt = Util.XORByteArray(SM3.encryptStringToByte(keyMerge.substring(16)), SM3.encryptStringToByte(String.valueOf(20 + 400)));
                int c = pointList.size();
                int n = 1;
                byte[] keySM3 = SM3.derivedKey(key, salt, c, n);
//                byte[] keySM3 = new byte[]{-101, 54, -72, 116, -105, -106, 43, -72, 125, 30, -16, -11, 71, -21, 96, 4, -74, 5, 101, -52, 33, -12, 94, -66, 14, -28, 58, 37, -60, -31, 82, -126};
                Double[][] ResList = Crypto4D.cryptoMethod(keySM3, pointList, pointList.size(), "DECRYPT");
                for (int j = 0; j < ResList.length; j++) {
                    Coordinate tmpCor = new Coordinate(ResList[j][0], ResList[j][1]);
                    org.locationtech.jts.geom.Point geoPoint = new GeometryFactory().createPoint(tmpCor);
                    try {
                        SimpleFeature fNew = writer.next();
                        fNew.setAttribute("geom", geoPoint);
                        writer.write();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            System.out.println("last:" + last);
            long enTime02 = System.currentTimeMillis();
            log.info("point解密时间：" + (enTime02 - enTime01));

            writer.close();
            dataStore.dispose();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private static class TaskerDe implements Callable<ThreeTuple<Integer, List, List<SimpleFeature>>> {
        private List<Double[]> pointList;
        private FeatureWriter<SimpleFeatureType, SimpleFeature> writer;

        public TaskerDe(List<Double[]> pointList, FeatureWriter<SimpleFeatureType, SimpleFeature> writer) {
            this.pointList = pointList;
            this.writer = writer;
        }

        @Override
        public ThreeTuple<Integer, List, List<SimpleFeature>> call() {
            String keyMerge = Tools.generateRandom(16);
            String keyString = keyMerge.substring(0, 16);
            byte[] key = keyString.getBytes(StandardCharsets.UTF_8);
            byte[] salt = Util.XORByteArray(SM3.encryptStringToByte(keyMerge.substring(16)), SM3.encryptStringToByte(String.valueOf(20 + 400)));
            int c = pointList.size();
            int n = 1;
            byte[] keySM3 = SM3.derivedKey(key, salt, c, n);
//                byte[] keySM3 = new byte[]{-101, 54, -72, 116, -105, -106, 43, -72, 125, 30, -16, -11, 71, -21, 96, 4, -74, 5, 101, -52, 33, -12, 94, -66, 14, -28, 58, 37, -60, -31, 82, -126};
            Double[][] ResList = Crypto4D.cryptoMethod(keySM3, pointList, pointList.size(), "DECRYPT");
            List<SimpleFeature> simpleFeatureList = new ArrayList<>();
            for (int j = 0; j < ResList.length; j++) {
                Coordinate tmpCor = new Coordinate(ResList[j][0], ResList[j][1]);
                org.locationtech.jts.geom.Point geoPoint = new GeometryFactory().createPoint(tmpCor);
                try {
                    SimpleFeature fNew = writer.next();
                    fNew.setAttribute("geom", geoPoint);
                    simpleFeatureList.add(fNew);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return new ThreeTuple<>(ResList.length, null, simpleFeatureList);
        }

    }

    private static boolean decryptMethodParallel(List<List<Double[]>> pointAttrList, String saveTableName, String keyFile, String priKeyFile, String outputPath) {
        try {
            //读取私钥数据
            BufferedReader in = new BufferedReader(new FileReader(priKeyFile));
            String priKey = in.readLine();
            //读取数据
            long enTime00 = System.currentTimeMillis();

            HashMap<String, SimpleFeature> shpMap = new HashMap<>();
            List<Entry<Integer, Geometry>> entryList = new ArrayList<>();

            DataStore dataStore = PostgisDataStore.getDataStore();
            SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
            builder.setName(saveTableName);
            builder.setCRS(null); // <- Coordinate reference system; null means default
            builder.add("id", Integer.class);
            builder.add("geom", org.locationtech.jts.geom.Point.class);
            SimpleFeatureType schema = builder.buildFeatureType();
            assert dataStore != null;
            // Check if the schema exists
            String typeName = schema.getTypeName();
            if (Arrays.asList(dataStore.getTypeNames()).contains(typeName)) {
                // If the schema exists, remove it
                dataStore.removeSchema(typeName);
            }
            dataStore.createSchema(schema);

            long enTime01 = System.currentTimeMillis();
            log.info("point文件读取时间：" + (enTime01 - enTime00));
            //读取索引数据

            FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriterAppend(schema.getTypeName(), Transaction.AUTO_COMMIT);

            List<Future<ThreeTuple<Integer, List, List<SimpleFeature>>>> futures = new ArrayList<>();

            ExecutorService threadPool = Executors.newFixedThreadPool(16);
            Future<ThreeTuple<Integer, List, List<SimpleFeature>>> res = null;
            for (List<Double[]> pointList : pointAttrList) {
                res = threadPool.submit(new TaskerDe(pointList, writer));
                futures.add(res);
            }
            threadPool.shutdown();
            while (!threadPool.awaitTermination(1, TimeUnit.MICROSECONDS)) {
//                System.out.println("线程池未关闭");
            }
            long enTime02 = System.currentTimeMillis();
            log.info("point解密时间：" + (enTime02 - enTime01));
            int count = 0;
            for (Future<ThreeTuple<Integer, List, List<SimpleFeature>>> future : futures) {
                ThreeTuple<Integer, List, List<SimpleFeature>> tt = future.get();
                count += 1;
            }
            System.out.println("count:" + count);
            writer.close();
            dataStore.dispose();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void main(String[] args) {
    }
}
