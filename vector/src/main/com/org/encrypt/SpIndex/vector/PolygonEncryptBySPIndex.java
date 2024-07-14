package encrypt.SpIndex.vector;

import com.github.davidmoten.rtree.*;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.geometry.Rectangle;
import org.apache.log4j.Logger;
import org.encrypt.SpIndex.db.PostgisDataStore;
import encrypt.SpIndex.lib.sm.sm3.SM3;
import org.encrypt.SpIndex.lib.sm.sm3.Util;
import encrypt.SpIndex.vector.kernel.Crypto4D;
import org.encrypt.SpIndex.vector.util.IO;
import org.encrypt.SpIndex.vector.util.ThreeTuple;
import org.encrypt.SpIndex.vector.util.Tools;
import org.geotools.data.DataStore;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.jts.geom.*;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

import static com.github.davidmoten.rtree.geometry.Geometries.rectangle;
import static org.encrypt.SpIndex.db.PostgisDataStore.saveRTree;

public class PolygonEncryptBySPIndex {

    public static Logger log = Logger.getLogger(PolygonEncryptBySPIndex.class.getClass());

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
            List<Double[]> pointList = new ArrayList<>();
            List<Integer> countList = new ArrayList<>();
            for (Entry leafRec : leafEntry) {
                Rectangle shpRec = (Rectangle) leafRec.geometry();
                SimpleFeature tmpFeature = shpMap.get(shpRec.toString());

                MultiPolygon tmpLine = (MultiPolygon) tmpFeature.getAttribute("geom");
                Coordinate[] tmpCor = tmpLine.getCoordinates();
                countList.add(tmpCor.length);
                for (Coordinate coordinate : tmpCor) {
                    Double[] corDouble = new Double[]{coordinate.x, coordinate.y};
                    pointList.add(corDouble);
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
                Coordinate[] tmpCorList = new Coordinate[countList.get(i) + 1];
                for (int j = 0; j < countList.get(i); j++) {
                    Coordinate tmpCor = new Coordinate(ResList[count1][0], ResList[count1][1]);
                    tmpCorList[j] = tmpCor;
                    count1++;
                }
                tmpCorList[tmpCorList.length - 1] = tmpCorList[0];
                Polygon geoPolygon = new GeometryFactory().createPolygon(tmpCorList);
                MultiPolygon polygonList = new GeometryFactory().createMultiPolygon(new Polygon[]{geoPolygon});
                tmpFeature.setAttribute("geom", polygonList);
                shpFeatures.add(tmpFeature);
                id.add((Integer) leafEntry.get(i).value());
            }

            return new ThreeTuple<>(pointList.size() + 1, id, shpFeatures);
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

            DataStore dataStore = PostgisDataStore.getDataStore();
            int count = PostgisDataStore.readGeomFromDB(dataStore, tableName, shpMap, entryList);

            long enTime01 = System.currentTimeMillis();
            log.info("polygon文件读取时间：" + (enTime01 - enTime00));

            //构建R树
            int maxChildren = 200;
            RTree<Integer, Geometry> tree = RTree.star().maxChildren(maxChildren).create(entryList);
            Node root = tree.root().get();
            List<Leaf> childrenList = Tools.traverseLeafPostOrder(root);
            log.info("叶子结点个数：" + childrenList.size());

            ExecutorService threadPool = Executors.newFixedThreadPool(16);
            List<Future<ThreeTuple<Integer, List, List<SimpleFeature>>>> futures = new ArrayList<>();
            Future<ThreeTuple<Integer, List, List<SimpleFeature>>> res;

            long enTime02 = System.currentTimeMillis();
            log.info("索引构建时间：" + (enTime02 - enTime01));

            String saltString = Tools.generateRandom(16);
            String password = Tools.generateRandom(16);

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
            log.info("polygon加密时间：" + (enTime03 - enTime02));

            saveRTree(outputPath + "Polygon", tree);
            String keyMerge = password + saltString;
            String outputFile = outputPath + "/" + tableName + "_encrypt.key";
            dataStore = PostgisDataStore.getDataStore();

            PostgisDataStore.writeGeomToDB(dataStore, tableName + "_encrypt", futures, MultiPolygon.class, keyMerge, pubKey, outputFile, count, rtreeIndexLeaf, keyList);
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
            log.info("polygon文件读取时间：" + (enTime01 - enTime00));

            //构建R树
            int maxChildren = 200;
            RTree<Integer, Geometry> tree = RTree.star().maxChildren(maxChildren).create(entryList);
            Node root = tree.root().get();
            List<Leaf> childrenList = Tools.traverseLeafPostOrder(root);
            log.info("叶子结点个数：" + childrenList.size());

            long enTime02 = System.currentTimeMillis();
            log.info("索引构建时间：" + (enTime02 - enTime01));

            String saltString = Tools.generateRandom(16);
            String password = Tools.generateRandom(16);

//            System.out.println(Arrays.toString(keySM3));
            List<List<SimpleFeature>> futures = new ArrayList<>();

            for (int i = 0; i < childrenList.size(); i++) {
                List<Entry> leafEntry = childrenList.get(i).entries();
                List<Double[]> pointList = new ArrayList<>();
                List<Integer> countList = new ArrayList<>();
                for (Entry leafRec : leafEntry) {
                    Rectangle shpRec = (Rectangle) leafRec.geometry();
                    SimpleFeature tmpFeature = shpMap.get(shpRec.toString());

                    MultiPolygon tmpLine = (MultiPolygon) tmpFeature.getAttribute("geom");
                    Coordinate[] tmpCor = tmpLine.getCoordinates();
                    countList.add(tmpCor.length);
                    for (Coordinate coordinate : tmpCor) {
                        Double[] corDouble = new Double[]{coordinate.x, coordinate.y};
                        pointList.add(corDouble);
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
                for (int j = 0; j < countList.size(); j++) {
                    Rectangle shpRec = (Rectangle) leafEntry.get(j).geometry();
                    SimpleFeature tmpFeature = SimpleFeatureBuilder.copy(shpMap.get(shpRec.toString()));
//                shpMapAttr.put(shpRec.toString(), tmpFeature.getAttributes());
                    Coordinate[] tmpCorList = new Coordinate[countList.get(j)];
                    for (int k = 0; k < countList.get(j); k++) {
                        Coordinate tmpCor = new Coordinate(ResList[count1][0], ResList[count1][1]);
                        tmpCorList[k] = tmpCor;
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
            log.info("polygon加密时间：" + (enTime03 - enTime02));

            System.out.println("futures length：" + futures.size());

            long enTime04 = System.currentTimeMillis();
            log.info("写入时间：" + (enTime04 - enTime03));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    public static List<List<MultiPolygon>> searchMethod(String indexFile, String tableName, Rectangle searchRec, HashMap<String, List<Integer>> rtreeIndexLeaf) {
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
            List<List<MultiPolygon>> pointAttrList = new ArrayList<>();
            System.out.println("要素个数：" + entryList.size());
            for (Leaf rec : searchLeafRes) {
                String searchRecString = rec.geometry().mbr().toString();
                List<Integer> singleResList = rtreeIndexLeaf.get(searchRecString);
                List<MultiPolygon> tmpList = new ArrayList<>();
                for (Integer list : singleResList) {
                    try {
                        MultiPolygon tmp = (MultiPolygon) shpMap.get(entryList.get(list).geometry().toString()).getAttribute("geom");
                        tmpList.add(tmp);
                    } catch (Exception e) {
//                        e.printStackTrace();
                    }
                }
                pointAttrList.add(tmpList);
            }
            long enTime1 = System.currentTimeMillis();
            log.info("polygon搜索时间：" + (enTime1 - enTime0));
            return pointAttrList;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static boolean decryptMethod(List<List<MultiPolygon>> polygonAttrList, String saveTableName, String keyFile, String priKeyFile, String outputPath) {
        try {
            //读取私钥数据
            BufferedReader in = new BufferedReader(new FileReader(priKeyFile));
            String priKey = in.readLine();

            log.info("start decrypt");
            long enTime00 = System.currentTimeMillis();

            DataStore dataStore = PostgisDataStore.getDataStore();
            SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
            builder.setName(saveTableName);
            builder.setCRS(null); // <- Coordinate reference system; null means default
            builder.add("id", Integer.class);
            builder.add("geom", Polygon.class);
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
            log.info("polygon文件读取时间：" + (enTime01 - enTime00));

            //设置Writer
            FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriterAppend(schema.getTypeName(), Transaction.AUTO_COMMIT);

//            System.out.println(Arrays.toString(keySM3));
            List<Double[]> pointList = new ArrayList<>();
            List<Integer> countList = new ArrayList<>();
            int last = 0;
            for (List<MultiPolygon> polygonList : polygonAttrList) {
                last += polygonList.size();
                for (MultiPolygon onePolygon : polygonList) {
                    Coordinate[] cor = onePolygon.getCoordinates();
                    for (int i = 0; i < cor.length - 1; i++) {
                        pointList.add(new Double[]{cor[i].x, cor[i].y});
                    }
                    countList.add(cor.length - 1);
                }
                String keyString = keyMerge.substring(0, 16);
                byte[] key = keyString.getBytes(StandardCharsets.UTF_8);
                byte[] salt = Util.XORByteArray(SM3.encryptStringToByte(keyMerge.substring(16)), SM3.encryptStringToByte(String.valueOf(20 + 400)));
                int c = 1024 + entry.getValue().size();
                int n = 1;
                byte[] keySM3 = SM3.derivedKey(key, salt, c, n);

                Double[][] ResList = Crypto4D.cryptoMethod(keySM3, pointList, polygonList.size(), "DECRYPT");
//                System.out.println(Arrays.deepToString(ResList));
                int count1 = 0;
                for (int i = 0; i < countList.size(); i++) {
                    Coordinate[] tmpCorList = new Coordinate[countList.get(i)];
                    for (int j = 0; j < countList.get(i); j++) {
                        Coordinate tmpCor = new Coordinate(ResList[count1][0], ResList[count1][1]);
                        tmpCorList[j] = tmpCor;
                        count1++;
                    }
                    tmpCorList[tmpCorList.length - 1] = tmpCorList[0];
                    Polygon geoPolygon = new GeometryFactory().createPolygon(tmpCorList);
                    try {
                        SimpleFeature feature = writer.next();
                        feature.setAttribute("geom", geoPolygon);
                        writer.write();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                pointList = new ArrayList<>();
                countList = new ArrayList<>();
            }

            System.out.println("last:" + last);
            long enTime02 = System.currentTimeMillis();
            log.info("polygon解密时间：" + (enTime02 - enTime01));
//
            writer.close();
            dataStore.dispose();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    private static class TaskerDe implements Callable<ThreeTuple<Integer, List, List<SimpleFeature>>> {
        private List<MultiPolygon> polygonList;
        private FeatureWriter<SimpleFeatureType, SimpleFeature> writer;

        public TaskerDe(List<MultiPolygon> polygonList, FeatureWriter<SimpleFeatureType, SimpleFeature> writer) {
            this.polygonList = polygonList;
            this.writer = writer;
        }

        @Override
        public ThreeTuple<Integer, List, List<SimpleFeature>> call() {
            List<Double[]> pointList = new ArrayList<>();
            List<Integer> countList = new ArrayList<>();
            for (MultiPolygon onePolygon : polygonList) {
                Coordinate[] cor = onePolygon.getCoordinates();
                for (int i = 0; i < cor.length - 1; i++) {
                    pointList.add(new Double[]{cor[i].x, cor[i].y});
                }
                countList.add(cor.length - 1);
            }
            String keyMerge = Tools.generateRandom(16);
            String keyString = keyMerge.substring(0, 16);
            byte[] key = keyString.getBytes(StandardCharsets.UTF_8);
            byte[] salt = Util.XORByteArray(SM3.encryptStringToByte(keyMerge.substring(16)), SM3.encryptStringToByte(String.valueOf(20 + 400)));
            int c = pointList.size();
            int n = 1;
            byte[] keySM3 = SM3.derivedKey(key, salt, c, n);

            Double[][] ResList = Crypto4D.cryptoMethod(keySM3, pointList, polygonList.size(), "DECRYPT");
//                System.out.println(Arrays.deepToString(ResList));
            int count1 = 0;
            List<SimpleFeature> simpleFeatureList = new ArrayList<>();
            for (int i = 0; i < countList.size(); i++) {
                Coordinate[] tmpCorList = new Coordinate[countList.get(i)];
                for (int j = 0; j < countList.get(i); j++) {
                    Coordinate tmpCor = new Coordinate(ResList[count1][0], ResList[count1][1]);
                    tmpCorList[j] = tmpCor;
                    count1++;
                }
                tmpCorList[tmpCorList.length - 1] = tmpCorList[0];
                Polygon geoPolygon = new GeometryFactory().createPolygon(tmpCorList);
                try {
                    SimpleFeature feature = writer.next();
                    feature.setAttribute("geom", geoPolygon);
                    simpleFeatureList.add(feature);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            return new ThreeTuple<>(pointList.size() + 1, null, simpleFeatureList);
        }

    }

    public static boolean decryptMethodParallel(List<List<MultiPolygon>> polygonAttrList, String saveTableName, String keyFile, String priKeyFile, String outputPath) {
        try {
            //读取私钥数据
            BufferedReader in = new BufferedReader(new FileReader(priKeyFile));
            String priKey = in.readLine();

            log.info("start decrypt");
            long enTime00 = System.currentTimeMillis();

            DataStore dataStore = PostgisDataStore.getDataStore();
            SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
            builder.setName(saveTableName);
            builder.setCRS(null); // <- Coordinate reference system; null means default
            builder.add("id", Integer.class);
            builder.add("geom", Polygon.class);
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
            log.info("polygon文件读取时间：" + (enTime01 - enTime00));

            //设置Writer
            FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriterAppend(schema.getTypeName(), Transaction.AUTO_COMMIT);

            List<Future<ThreeTuple<Integer, List, List<SimpleFeature>>>> futures = new ArrayList<>();

            ExecutorService threadPool = Executors.newFixedThreadPool(16);
            Future<ThreeTuple<Integer, List, List<SimpleFeature>>> res = null;
            for (List<MultiPolygon> polygonList : polygonAttrList) {
                res = threadPool.submit(new TaskerDe(polygonList, writer));
                futures.add(res);
            }
            threadPool.shutdown();
//            while (!threadPool.awaitTermination(1, TimeUnit.MICROSECONDS)) {
////                System.out.println("线程池未关闭");
//            }
            long enTime02 = System.currentTimeMillis();
            log.info("polygon解密时间：" + (enTime02 - enTime01));
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

