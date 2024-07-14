package encrypt.SpIndex.vector;

import com.github.davidmoten.rtree.*;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.geometry.Rectangle;
import encrypt.SpIndex.vector.kernel.Crypto4D;
import org.apache.log4j.Logger;
import org.encrypt.SpIndex.db.PostgisDataStore;
import encrypt.SpIndex.lib.sm.sm3.SM3;
import org.encrypt.SpIndex.lib.sm.sm3.Util;
import org.encrypt.SpIndex.vector.util.*;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.jts.geom.*;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

import static com.github.davidmoten.rtree.geometry.Geometries.rectangle;
import static org.encrypt.SpIndex.db.PostgisDataStore.saveRTree;

public class LineEncryptBySPIndex {
    public static Logger log = Logger.getLogger(LineEncryptBySPIndex.class.getClass());

    private static class Tasker implements Callable<ThreeTuple<Integer, List, List<SimpleFeature>>> {
        private Leaf childLeaf;
        private HashMap<String, SimpleFeature> shpMap;
        String saltString;
        String password;

        public Tasker(Leaf childLeaf, HashMap<String, SimpleFeature> shpMap, String saltString, String password) {
            this.childLeaf = childLeaf;
            this.shpMap = shpMap;
            this.saltString = saltString;
            this.password = password;
        }

        @Override
        public ThreeTuple<Integer, List, List<SimpleFeature>> call() {
            List<Entry> leafEntry = childLeaf.entries();
            List<Double[]> pointList = new ArrayList<>();
            //记录每条线的点数
            List<Integer> countList = new ArrayList<>();
            for (Entry leafRec : leafEntry) {
                Rectangle shpRec = (Rectangle) leafRec.geometry();
                SimpleFeature tmpFeature = shpMap.get(shpRec.toString());
                try {
                    MultiLineString tmpLine = (MultiLineString) tmpFeature.getAttribute("geom");
                    Coordinate[] tmpCor = tmpLine.getCoordinates();
                    countList.add(tmpCor.length);
                    Double[] corDouble;

                    for (int i = 0; i < tmpCor.length; i++) {
                        corDouble = new Double[]{tmpCor[i].x, tmpCor[i].y};
                        pointList.add(corDouble);
                    }
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

            List<SimpleFeature> shpFeatures = new ArrayList<>();
            int count1 = 0;
            List<Integer> id = new ArrayList<>();

            for (int i = 0; i < countList.size(); i++) {
                Rectangle shpRec = (Rectangle) leafEntry.get(i).geometry();
                SimpleFeature tmpFeature = SimpleFeatureBuilder.copy(shpMap.get(shpRec.toString()));
                Coordinate[] tmpCorList = new Coordinate[countList.get(i)];
                for (int j = 0; j < countList.get(i); j++) {
                    Coordinate tmpCor = new Coordinate(ResList[count1][0], ResList[count1][1]);
                    tmpCorList[j] = tmpCor;
                    count1++;
                }
                LineString geoLine = new GeometryFactory().createLineString(tmpCorList);
                MultiLineString lineList = new GeometryFactory().createMultiLineString(new LineString[]{geoLine});
                tmpFeature.setAttribute("geom", lineList);

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
            int count = PostgisDataStore.readGeomFromDB(dataStore, tableName, shpMap, entryList);

            long enTime01 = System.currentTimeMillis();
            log.info("line文件读取时间：" + (enTime01 - enTime00));

//          构建R树
            int maxChildren = 20;
            RTree<Integer, Geometry> tree = RTree.maxChildren(maxChildren).create(entryList);
//            tree.visualize(10000, 10000).save("data/vector/line/rtree.png");
            Node root = tree.root().get();
            List<Leaf> childrenList = Tools.traverseLeafPostOrder(root);
            log.info("叶子结点个数：" + childrenList.size());


            String saltString = Tools.generateRandom(16);
            String password = Tools.generateRandom(16);

            List<Future<ThreeTuple<Integer, List, List<SimpleFeature>>>> futures = new ArrayList<>();

            ExecutorService threadPool = Executors.newFixedThreadPool(16);
            Future<ThreeTuple<Integer, List, List<SimpleFeature>>> res;
            long enTime02 = System.currentTimeMillis();
            log.info("索引构建时间：" + (enTime02 - enTime01));
            List<String> keyList = new ArrayList<>();
            for (Leaf childLeaf : childrenList) {
                res = threadPool.submit(new Tasker(childLeaf, shpMap, saltString, password));
                futures.add(res);
                Rectangle leafRecBon = (Rectangle) childLeaf.geometry();
                keyList.add(leafRecBon.toString());
            }
            threadPool.shutdown();
            while (!threadPool.awaitTermination(1, TimeUnit.MICROSECONDS)) {
            }
            long enTime03 = System.currentTimeMillis();
            log.info("line加密时间：" + (enTime03 - enTime02));

            saveRTree(outputPath + "Line", tree);
            String keyMerge = password + saltString;
            String outputFile = outputPath + "/" + tableName + "_encrypt.key";
//            String outputFile = outputPath + "/test.shp";
            dataStore = PostgisDataStore.getDataStore();

            PostgisDataStore.writeGeomToDB(dataStore, tableName + "_encrypt", futures, MultiLineString.class, keyMerge, pubKey, outputFile, count, rtreeIndexLeaf, keyList);
            long enTime04 = System.currentTimeMillis();
            log.info("写入时间：" + (enTime04 - enTime03));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean encryptMethodSingle(String tableName, String pubKeyFile, String outputPath) {
        try {
            //读取公钥数据
            BufferedReader in = new BufferedReader(new FileReader(pubKeyFile));
            String pubKey = in.readLine();

            //读取数据
            long enTime00 = System.currentTimeMillis();

            HashMap<String, SimpleFeature> shpMap = new HashMap<>();
            List<Entry<Integer, Geometry>> entryList = new ArrayList<>();
//            IO.readPointShapefile(shapeDS, shpMap, entryList);

            DataStore dataStore = PostgisDataStore.getDataStore();
            int count = PostgisDataStore.readGeomFromDB(dataStore, tableName, shpMap, entryList);

            long enTime01 = System.currentTimeMillis();
            log.info("line文件读取时间：" + (enTime01 - enTime00));
//          构建R树
            int maxChildren = 200;
            RTree<Integer, Geometry> tree = RTree.maxChildren(maxChildren).create(entryList);
//            tree.visualize(10000, 10000).save("data/vector/line/rtree.png");
            Node root = tree.root().get();
            List<Leaf> childrenList = Tools.traverseLeafPostOrder(root);
            log.info("叶子结点个数：" + childrenList.size());

            String saltString = Tools.generateRandom(16);
            String password = Tools.generateRandom(16);

            List<List<SimpleFeature>> futures = new ArrayList<>();

            long enTime02 = System.currentTimeMillis();
            log.info("索引构建时间：" + (enTime02 - enTime01));
            for (Leaf childLeaf : childrenList) {
                List<Entry> leafEntry = childLeaf.entries();
                List<Double[]> pointList = new ArrayList<>();
                //记录每条线的点数
                List<Integer> countList = new ArrayList<>();
                for (Entry leafRec : leafEntry) {
                    Rectangle shpRec = (Rectangle) leafRec.geometry();
                    SimpleFeature tmpFeature = shpMap.get(shpRec.toString());
                    try {
                        MultiLineString tmpLine = (MultiLineString) tmpFeature.getAttribute("geom");
                        Coordinate[] tmpCor = tmpLine.getCoordinates();
                        countList.add(tmpCor.length);
                        Double[] corDouble;

                        for (int i = 0; i < tmpCor.length; i++) {
                            corDouble = new Double[]{tmpCor[i].x, tmpCor[i].y};
                            pointList.add(corDouble);
                        }
                    } catch (Exception e) {
//                        log.info(e);
                        System.out.println(e);
                    }
                }
                byte[] key = password.getBytes(StandardCharsets.UTF_8);
                byte[] salt = Util.XORByteArray(SM3.encryptStringToByte(saltString), SM3.encryptStringToByte(String.valueOf(20 + 400)));
                int c = leafEntry.size();
                int n = 1;
                byte[] keySM3 = SM3.derivedKey(key, salt, c, n);

                Double[][] ResList = Crypto4D.cryptoMethod(keySM3, pointList, leafEntry.size(), "ENCRYPT");

                List<SimpleFeature> shpFeatures = new ArrayList<>();
                int count1 = 0;
                for (int i = 0; i < countList.size(); i++) {
                    Rectangle shpRec = (Rectangle) leafEntry.get(i).geometry();
                    SimpleFeature tmpFeature = SimpleFeatureBuilder.copy(shpMap.get(shpRec.toString()));
                    Coordinate[] tmpCorList = new Coordinate[countList.get(i)];
                    for (int j = 0; j < countList.get(i); j++) {
                        Coordinate tmpCor = new Coordinate(ResList[count1][0], ResList[count1][1]);
                        tmpCorList[j] = tmpCor;
                        count1++;
                    }
                    LineString geoLine = new GeometryFactory().createLineString(tmpCorList);
                    MultiLineString lineList = new GeometryFactory().createMultiLineString(new LineString[]{geoLine});
                    tmpFeature.setAttribute("geom", lineList);

                    shpFeatures.add(tmpFeature);
                }
                futures.add(shpFeatures);
            }

            long enTime03 = System.currentTimeMillis();
            log.info("line加密时间：" + (enTime03 - enTime02));

            System.out.println("futures length：" + futures.size());

            long enTime04 = System.currentTimeMillis();
            log.info("写入时间：" + (enTime04 - enTime03));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static List<List<MultiLineString>> searchMethod(String indexFile, String tableName, Rectangle searchRec, HashMap<String, List<Integer>> rtreeIndexLeaf) {
        try {
            //读取数据
            Serializer<Integer, Geometry> serializer = Serializers.flatBuffers().javaIo();
            File file = new File(indexFile);
            InputStream is = new FileInputStream(file);
            RTree<Integer, Geometry> tree = serializer.read(is, file.length(), InternalStructure.DEFAULT);
            HashMap<String, SimpleFeature> shpMap = new HashMap<>();

            DataStore dataStore = PostgisDataStore.getDataStore();
            SimpleFeatureSource source = dataStore.getFeatureSource(tableName);
            int count = source.getCount(Query.ALL);
            List<Entry<Integer, Geometry>> entryList = new ArrayList<>(Collections.nCopies(count, null));
            PostgisDataStore.readGeomFromDBSearch(dataStore, tableName, shpMap, entryList);

            long enTime0 = System.currentTimeMillis();
            List<Leaf> searchLeafRes = Tools.intersection(searchRec, tree.root().get());
            System.out.println("查找叶子结点个数：" + searchLeafRes.size());
            List<List<MultiLineString>> pointAttrList = new ArrayList<>();
            for (Leaf rec : searchLeafRes) {
                String searchRecString = rec.geometry().mbr().toString();
                List<Integer> singleResList = rtreeIndexLeaf.get(searchRecString);

                List<MultiLineString> tmpList = new ArrayList<>();
                for (Integer list : singleResList) {
                    try {
                        MultiLineString tmp = (MultiLineString) shpMap.get(entryList.get(list).geometry().toString()).getAttribute("geom");
                        tmpList.add(tmp);
                    } catch (Exception e) {
//                        e.printStackTrace();
                    }
                }
                pointAttrList.add(tmpList);
            }
            long enTime1 = System.currentTimeMillis();
            log.info("line搜索时间：" + (enTime1 - enTime0));
            return pointAttrList;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static boolean decryptMethod(List<List<MultiLineString>> lineAttrList, String saveTableName, String keyFile, String priKeyFile, String outputPath) {
        try {
            //读取私钥数据
            BufferedReader in = new BufferedReader(new FileReader(priKeyFile));
            String priKey = in.readLine();

            long enTime00 = System.currentTimeMillis();

            long enTime01 = System.currentTimeMillis();
            log.info("line文件读取时间：" + (enTime01 - enTime00));
            //读取索引数据

            DataStore dataStore = PostgisDataStore.getDataStore();
            SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
            builder.setName(saveTableName);
            builder.setCRS(null); // <- Coordinate reference system; null means default
//            builder.add("id", Integer.class);
            builder.add("geom", MultiLineString.class);
            SimpleFeatureType schema = builder.buildFeatureType();

            assert dataStore != null;
            // Check if the schema exists
            String typeName = schema.getTypeName();
            if (Arrays.asList(dataStore.getTypeNames()).contains(typeName)) {
                // If the schema exists, remove it
                dataStore.removeSchema(typeName);
            }
            dataStore.createSchema(schema);

            //设置Writer
            FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriterAppend(schema.getTypeName(), Transaction.AUTO_COMMIT);



            long enTime000 = System.currentTimeMillis();
            log.info("构造时间：" + (enTime000 - enTime01));

            int last = 0;
            for (List<MultiLineString> lineList : lineAttrList) {
                List<Integer> countList = new ArrayList<>();
                List<Double[]> pointList = new ArrayList<>();
                last += lineList.size();
                for (MultiLineString line : lineList) {
                    for (Coordinate geoPoint : line.getCoordinates()) {
                        pointList.add(new Double[]{geoPoint.x, geoPoint.y});
                    }
                    countList.add(line.getCoordinates().length);
                }

                String keyMerge = Tools.generateRandom(16);
                String keyString = keyMerge.substring(0, 16);
                byte[] key = keyString.getBytes(StandardCharsets.UTF_8);
                byte[] salt = Util.XORByteArray(SM3.encryptStringToByte(keyMerge.substring(16)), SM3.encryptStringToByte(String.valueOf(20 + 400)));
                int c = 500 + lineList.size();
                int n = 1;
                byte[] keySM3 = SM3.derivedKey(key, salt, c, n);

                Double[][] ResList = Crypto4D.cryptoMethod(keySM3, pointList, lineList.size(), "DECRYPT");

                int count = 0;
                for (int ii = 0; ii < countList.size(); ii++) {
                    Coordinate[] tmpCorList = new Coordinate[countList.get(ii)];
                    for (int j = 0; j < countList.get(ii); j++) {
                        Coordinate tmpCor = new Coordinate(ResList[count][0], ResList[count][1]);
                        tmpCorList[j] = tmpCor;
                        count++;
                    }
                    LineString geoLine = new GeometryFactory().createLineString(tmpCorList);
                    MultiLineString newLineList = new GeometryFactory().createMultiLineString(new LineString[]{geoLine});
//
                    try {
                        SimpleFeature feature = writer.next();
                        feature.setAttribute("geom", newLineList);
                        writer.write();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
//            System.out.println("last:" + last);
            long enTime02 = System.currentTimeMillis();
            log.info("line解密时间：" + (enTime02 - enTime000));

            writer.close();
            dataStore.dispose();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private static class TaskerDe implements Callable<ThreeTuple<Integer, List, List<SimpleFeature>>> {
        private List<MultiLineString> lineList;
        private FeatureWriter<SimpleFeatureType, SimpleFeature> writer;

        public TaskerDe(List<MultiLineString> lineList, FeatureWriter<SimpleFeatureType, SimpleFeature> writer) {
            this.lineList = lineList;
            this.writer = writer;
        }

        @Override
        public ThreeTuple<Integer, List, List<SimpleFeature>> call() {
            List<Integer> countList = new ArrayList<>();
            List<Double[]> pointList = new ArrayList<>();
            for (MultiLineString line : lineList) {
                for (Coordinate geoPoint : line.getCoordinates()) {
                    pointList.add(new Double[]{geoPoint.x, geoPoint.y});
                }
                countList.add(line.getCoordinates().length);
            }

            String keyMerge = Tools.generateRandom(16);
            String keyString = keyMerge.substring(0, 16);
            byte[] key = keyString.getBytes(StandardCharsets.UTF_8);
            byte[] salt = Util.XORByteArray(SM3.encryptStringToByte(keyMerge.substring(16)), SM3.encryptStringToByte(String.valueOf(20 + 400)));
            int c = 500 + lineList.size();
            int n = 1;
            byte[] keySM3 = SM3.derivedKey(key, salt, c, n);

            Double[][] ResList = Crypto4D.cryptoMethod(keySM3, pointList, lineList.size(), "DECRYPT");
            List<SimpleFeature> simpleFeatureList = new ArrayList<>();
            int count = 0;
            for (int ii = 0; ii < countList.size(); ii++) {
                Coordinate[] tmpCorList = new Coordinate[countList.get(ii)];
                for (int j = 0; j < countList.get(ii); j++) {
                    Coordinate tmpCor = new Coordinate(ResList[count][0], ResList[count][1]);
                    tmpCorList[j] = tmpCor;
                    count++;
                }
                LineString geoLine = new GeometryFactory().createLineString(tmpCorList);
                MultiLineString newLineList = new GeometryFactory().createMultiLineString(new LineString[]{geoLine});

                try {
                    SimpleFeature feature = writer.next();
                    feature.setAttribute("geom", newLineList);
                    simpleFeatureList.add(feature);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return new ThreeTuple<>(pointList.size(), null, simpleFeatureList);
        }

    }

    public static boolean decryptMethodParallel(List<List<MultiLineString>> lineAttrList, String saveTableName, String keyFile, String priKeyFile, String outputPath) {
        try {
            long enTime01 = System.currentTimeMillis();
            //读取索引数据

            DataStore dataStore = PostgisDataStore.getDataStore();
            SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
            builder.setName(saveTableName);
            builder.setCRS(null); // <- Coordinate reference system; null means default
//            builder.add("id", Integer.class);
            builder.add("geom", MultiLineString.class);
            SimpleFeatureType schema = builder.buildFeatureType();

            assert dataStore != null;
            // Check if the schema exists
            String typeName = schema.getTypeName();
            if (Arrays.asList(dataStore.getTypeNames()).contains(typeName)) {
                // If the schema exists, remove it
                dataStore.removeSchema(typeName);
            }
            dataStore.createSchema(schema);

            //设置Writer
            FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriterAppend(schema.getTypeName(), Transaction.AUTO_COMMIT);

            long enTime000 = System.currentTimeMillis();
            log.info("构造时间：" + (enTime000 - enTime01));

            List<Future<ThreeTuple<Integer, List, List<SimpleFeature>>>> futures = new ArrayList<>();

            ExecutorService threadPool = Executors.newFixedThreadPool(16);
            Future<ThreeTuple<Integer, List, List<SimpleFeature>>> res = null;
            for (List<MultiLineString> lineList : lineAttrList) {
                res = threadPool.submit(new TaskerDe(lineList, writer));
                futures.add(res);
            }
            threadPool.shutdown();
            while (!threadPool.awaitTermination(1, TimeUnit.MICROSECONDS)) {
//                System.out.println("线程池未关闭");
            }
            long enTime02 = System.currentTimeMillis();
            log.info("line解密时间：" + (enTime02 - enTime000));
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

    public static boolean decryptMethodSecurity(List<List<MultiLineString>> lineAttrList, String saveTableName, String keyFile, String priKeyFile, double percentage) {
        try {
            //读取私钥数据
            BufferedReader in = new BufferedReader(new FileReader(priKeyFile));
            String priKey = in.readLine();

            long enTime00 = System.currentTimeMillis();

            long enTime01 = System.currentTimeMillis();
            log.info("line文件读取时间：" + (enTime01 - enTime00));
            //读取索引数据

            DataStore dataStore = PostgisDataStore.getDataStore();
            SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
            builder.setName(saveTableName);
            builder.setCRS(null); // <- Coordinate reference system; null means default
//            builder.add("id", Integer.class);
            builder.add("geom", MultiLineString.class);
            SimpleFeatureType schema = builder.buildFeatureType();

            assert dataStore != null;
            // Check if the schema exists
            String typeName = schema.getTypeName();
            if (Arrays.asList(dataStore.getTypeNames()).contains(typeName)) {
                // If the schema exists, remove it
                dataStore.removeSchema(typeName);
            }
            dataStore.createSchema(schema);

            //设置Writer
            FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriterAppend(schema.getTypeName(), Transaction.AUTO_COMMIT);


            int num = 0;
            for (List<MultiLineString> lineList : lineAttrList) {
                num += lineList.size();
            }
//            double percentage = 0.1; // 设置百分比
            int numberOfOperations = (int) (num * percentage);

            Random random = new Random();
            List<Integer> indicesToOperate = new ArrayList<>();

            // 生成需要操作的元素的索引列表
            while (indicesToOperate.size() < numberOfOperations) {
                int index = random.nextInt(num);
                if (!indicesToOperate.contains(index)) {
                    indicesToOperate.add(index);
                }
            }

            long enTime000 = System.currentTimeMillis();
            log.info("构造时间：" + (enTime000 - enTime01));

            int last = 0;
            for (List<MultiLineString> lineList : lineAttrList) {
                List<Integer> countList = new ArrayList<>();
                List<Double[]> pointList = new ArrayList<>();
                last += lineList.size();
                for (MultiLineString line : lineList) {
                    if (indicesToOperate.contains(last)){
                        for (Coordinate ignored : line.getCoordinates()) {
                            pointList.add(new Double[]{random.nextDouble(), random.nextDouble()});
                        }
                    }
                    else{
                        for (Coordinate geoPoint : line.getCoordinates()) {
                            pointList.add(new Double[]{geoPoint.x, geoPoint.y});
                        }
                    }
                    countList.add(line.getCoordinates().length);
                }

                String keyMerge = Tools.generateRandom(16);
                String keyString = keyMerge.substring(0, 16);
                byte[] key = keyString.getBytes(StandardCharsets.UTF_8);
                byte[] salt = Util.XORByteArray(SM3.encryptStringToByte(keyMerge.substring(16)), SM3.encryptStringToByte(String.valueOf(20 + 400)));
                int c = 500 + lineList.size();
                int n = 1;
                byte[] keySM3 = SM3.derivedKey(key, salt, c, n);

                Double[][] ResList = Crypto4D.cryptoMethod(keySM3, pointList, lineList.size(), "DECRYPT");

                int count = 0;
                for (int ii = 0; ii < countList.size(); ii++) {
                    Coordinate[] tmpCorList = new Coordinate[countList.get(ii)];
                    for (int j = 0; j < countList.get(ii); j++) {
                        Coordinate tmpCor = new Coordinate(ResList[count][0], ResList[count][1]);
                        tmpCorList[j] = tmpCor;
                        count++;
                    }
                    LineString geoLine = new GeometryFactory().createLineString(tmpCorList);
                    MultiLineString newLineList = new GeometryFactory().createMultiLineString(new LineString[]{geoLine});
//
                    try {
                        SimpleFeature feature = writer.next();
                        feature.setAttribute("geom", newLineList);
                        writer.write();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
//            System.out.println("last:" + last);
            long enTime02 = System.currentTimeMillis();
            log.info("line解密时间：" + (enTime02 - enTime000));

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
