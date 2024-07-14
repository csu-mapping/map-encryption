package encrypt.SpIndex.vector;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.Leaf;
import com.github.davidmoten.rtree.Node;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.geometry.Rectangle;
import encrypt.SpIndex.vector.kernel.Crypto4D;
import org.apache.log4j.Logger;
import org.encrypt.SpIndex.db.PostgisDataStore;
import org.encrypt.SpIndex.vector.util.ThreeTuple;
import org.encrypt.SpIndex.vector.util.Tools;
import org.geotools.data.DataStore;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.jts.geom.*;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;

public class EncryptBySPIndex {
    public static Logger log = Logger.getLogger(Class.class);

    @FunctionalInterface
    interface TaskerFactory {
        Callable<ThreeTuple<Integer, Integer, List<SimpleFeature>>> create(Leaf childLeaf, HashMap<String, SimpleFeature> shpMap, String saltString, String password);
    }

    private static class PointTasker implements Callable<ThreeTuple<Integer, Integer, List<SimpleFeature>>> {
        private Leaf childLeaf;
        private HashMap<String, SimpleFeature> shpMap;
        private String saltString;
        private String password;

        public PointTasker(Leaf childLeaf, HashMap<String, SimpleFeature> shpMap, String saltString, String password) {
            this.childLeaf = childLeaf;
            this.shpMap = shpMap;
            this.saltString = saltString;
            this.password = password;
        }

        @Override
        public ThreeTuple<Integer, Integer, List<SimpleFeature>> call() {
//            System.out.println("PointTasker");
            List<Entry> leafEntry = childLeaf.entries();
            List<SimpleFeature> shpFeatures = new ArrayList<>();
            List<Double[]> pointList = new ArrayList<>();
            for (Entry leafRec : leafEntry) {
                com.github.davidmoten.rtree.geometry.Point shpRec = (com.github.davidmoten.rtree.geometry.Point) leafRec.geometry();
                SimpleFeature tmpFeature = shpMap.get(shpRec.toString());
                try {
//                    System.out.println(tmpFeature.getAttribute("geom"));
                    Point tmpLine = (Point) tmpFeature.getAttribute("geom");
                    Double[] corDouble = new Double[]{tmpLine.getX(), tmpLine.getY()};
                    pointList.add(corDouble);
                } catch (Exception e) {
                    log.info(e);
                }
            }
            byte[] key = password.getBytes(StandardCharsets.UTF_8);
            byte[] salt = Util.XORByteArray(SM3.encryptStringToByte(saltString), SM3.encryptStringToByte(String.valueOf(20 + 400)));
            int c = 1024 + leafEntry.size();
            int n = 1;
            byte[] keySM3 = SM3.derivedKey(key, salt, c, n);
			
            Double[][] ResList = Crypto4D.cryptoMethod(keySM3, pointList, leafEntry.size(), "ENCRYPT");

            for (int i = 0; i < ResList.length; i++) {
                Rectangle shpRec = (Rectangle) leafEntry.get(i).geometry();
                SimpleFeature tmpFeature = SimpleFeatureBuilder.copy(shpMap.get(shpRec.toString()));
                Coordinate tmpCor = new Coordinate(ResList[i][0], ResList[i][1]);

                Point geoPoint = new GeometryFactory().createPoint(tmpCor);
                tmpFeature.setAttribute("geom", geoPoint);
                shpFeatures.add(tmpFeature);
            }
            return new ThreeTuple<>(pointList.size(), n, shpFeatures);
        }

    }

    private static class LineTasker implements Callable<ThreeTuple<Integer, Integer, List<SimpleFeature>>> {
        private Leaf childLeaf;
        private HashMap<String, SimpleFeature> shpMap;
        String saltString;
        String password;

        public LineTasker(Leaf childLeaf, HashMap<String, SimpleFeature> shpMap, String saltString, String password) {
            this.childLeaf = childLeaf;
            this.shpMap = shpMap;
            this.saltString = saltString;
            this.password = password;
        }

        @Override
        public ThreeTuple<Integer, Integer, List<SimpleFeature>> call() {
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
            int c = 1024 + leafEntry.size();
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
            return new ThreeTuple<>(pointList.size(), n, shpFeatures);
        }

    }

    private static class PolygonTasker implements Callable<ThreeTuple<Integer, Integer, List<SimpleFeature>>> {
        private Leaf childLeaf;
        private HashMap<String, SimpleFeature> shpMap;
        private String saltString;
        private String password;

        public PolygonTasker(Leaf childLeaf, HashMap<String, SimpleFeature> shpMap, String saltString, String password) {
            this.childLeaf = childLeaf;
            this.shpMap = shpMap;
            this.saltString = saltString;
            this.password = password;
        }

        @Override
        public ThreeTuple<Integer, Integer, List<SimpleFeature>> call() {
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
            int c = 1024 + leafEntry.size();
            int n = 1;
            byte[] keySM3 = SM3.derivedKey(key, salt, c, n);

            Double[][] ResList = Crypto4D.cryptoMethod(keySM3, pointList, leafEntry.size(), "ENCRYPT");

            List<SimpleFeature> shpFeatures = new ArrayList<>();
            int count1 = 0;
            for (int i = 0; i < countList.size(); i++) {
                Rectangle shpRec = (Rectangle) leafEntry.get(i).geometry();
                SimpleFeature tmpFeature = SimpleFeatureBuilder.copy(shpMap.get(shpRec.toString()));
//                shpMapAttr.put(shpRec.toString(), tmpFeature.getAttributes());
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

            return new ThreeTuple<>(pointList.size() + 1, n, shpFeatures);
        }

    }

    private static boolean encryptParallel(String tableName, String pubKeyFile, String outputPath) {
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
            assert dataStore != null;
            SimpleFeatureType sft = dataStore.getSchema(tableName);
            Class<?> geomType = sft.getGeometryDescriptor().getType().getBinding();

            PostgisDataStore.readGeomFromDB(dataStore, tableName, shpMap, entryList);

            TaskerFactory taskerFactory = null;

            if (Point.class.isAssignableFrom(geomType)) {
                taskerFactory = PointTasker::new;
            } else if (MultiLineString.class.isAssignableFrom(geomType)) {
                taskerFactory = LineTasker::new;
            } else if (MultiPolygon.class.isAssignableFrom(geomType)) {
                taskerFactory = PolygonTasker::new;
            } else {
                System.out.println("The layer type is something else: " + geomType.getName());
            }

            long enTime01 = System.currentTimeMillis();
            log.info("point文件读取时间：" + (enTime01 - enTime00));

//          构建R树
            int maxChildren = 2000;
            RTree<Integer, Geometry> tree = RTree.star().maxChildren(maxChildren).create(entryList);
            Node<Integer, Geometry> root = tree.root().get();
            //遍历得到叶子节点
            List<Leaf> childrenList = Tools.traverseLeafPostOrder(root);
            log.info("叶子结点个数：" + childrenList.size());

            long enTime02 = System.currentTimeMillis();
            log.info("索引构建时间：" + (enTime02 - enTime01));

            String saltString = Tools.generateRandom(16);
            String password = Tools.generateRandom(16);

            List<Future<ThreeTuple<Integer, Integer, List<SimpleFeature>>>> futures = new ArrayList<>();

            ExecutorService threadPool = Executors.newFixedThreadPool(16);
            Future<ThreeTuple<Integer, Integer, List<SimpleFeature>>> res = null;
            for (Leaf childLeaf : childrenList) {
                assert taskerFactory != null;
                Callable<ThreeTuple<Integer, Integer, List<SimpleFeature>>> taskerInstance = taskerFactory.create(childLeaf, shpMap, saltString, password);
                res = threadPool.submit(taskerInstance);
                futures.add(res);
            }
            threadPool.shutdown();

            long enTime03 = System.currentTimeMillis();
            log.info("point加密时间：" + (enTime03 - enTime02));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void main(String[] args) {
    }
}
