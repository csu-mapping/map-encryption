package encrypt.SpIndex.db;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.Serializer;
import com.github.davidmoten.rtree.Serializers;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import org.encrypt.SpIndex.lib.sm.sm2.SM2Utils;
import org.encrypt.SpIndex.lib.sm.sm2.SecurityUtils;
import org.encrypt.SpIndex.vector.util.SerializeUtils;
import org.encrypt.SpIndex.vector.util.ThreeTuple;
import org.encrypt.SpIndex.vector.util.Tools;
import org.encrypt.SpIndex.vector.util.TwoTuple;
import org.geotools.data.*;
import org.geotools.data.postgis.PostgisNGDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.util.concurrent.Future;

import static com.github.davidmoten.rtree.Entries.entry;


public class PostgisDataStore {

    public static DataStore getDataStore() throws IOException {
        Map<String, Object> params = new HashMap<>();

        Properties pros = new Properties();

        // 读取数据库连接配置文件
        try (InputStream input = db_test.class.getClassLoader().getResourceAsStream("encrypt.SpIndex.db.properties")) {
            if (input == null) {
                System.out.println("Sorry, unable to find encrypt.SpIndex.db.properties");
                return null;
            }
            pros.load(input);
            params.put(PostgisNGDataStoreFactory.DBTYPE.key, pros.getProperty("dbtype"));
            params.put(PostgisNGDataStoreFactory.HOST.key, pros.getProperty("host"));
            params.put(PostgisNGDataStoreFactory.PORT.key, pros.getProperty("port"));
            params.put(PostgisNGDataStoreFactory.SCHEMA.key, pros.getProperty("schema"));
            params.put(PostgisNGDataStoreFactory.DATABASE.key, pros.getProperty("database"));
            params.put(PostgisNGDataStoreFactory.USER.key, pros.getProperty("user"));
            params.put(PostgisNGDataStoreFactory.PASSWD.key, pros.getProperty("password"));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        DataStore dataStore = DataStoreFinder.getDataStore(params);
        if (dataStore == null) {
            System.out.println("Connection failed");
            return null;
        }
        return dataStore;
    }

    public static int readGeomFromDB(DataStore dataStore, String tableName, HashMap<String, SimpleFeature> shpMap, List<Entry<Integer, Geometry>> entryList) {
        try {
            SimpleFeatureSource source = dataStore.getFeatureSource(tableName);
//            int count = source.getCount(Query.ALL);

            Filter filter = Filter.INCLUDE; // this is an empty filter - fetches all features
            Query query = new Query(tableName, filter);
            SimpleFeatureCollection features = source.getFeatures(query);
            if (features == null) return 0;
            //读取数据
            SimpleFeatureIterator iterator = features.features();
            int id = 0;
//            iterator.hasNext()
            while (iterator.hasNext()) {
                SimpleFeature feature = iterator.next();

                try {
                    org.locationtech.jts.geom.Geometry geomCollection = (org.locationtech.jts.geom.Geometry) feature.getDefaultGeometryProperty().getValue();
                    for (int i = 0; i < geomCollection.getNumGeometries(); i++) {
                        org.locationtech.jts.geom.Geometry geom = geomCollection.getGeometryN(i);
                        if (geom.getCoordinates().length < 1) continue;
                        com.github.davidmoten.rtree.geometry.Geometry rtCol = null;
                        if (geom instanceof Point) {
                            Point pro = (Point) geom;
                            rtCol = Geometries.point(pro.getX(), pro.getY());
                        } else if (geom instanceof LineString) {
                            LineString pro = (LineString) geom;
                            rtCol = Tools.getGeoCod(pro.getBoundary());
                        } else if (geom instanceof Polygon) {
                            Polygon pro = (Polygon) geom;
                            rtCol = Tools.getGeoCod(pro.getBoundary());
                        }
                        assert rtCol != null;
                        if (!shpMap.containsKey(rtCol.toString())) {
                            shpMap.put(rtCol.toString(), feature);
                            entryList.add(entry(id, rtCol));
                            id++;
                        }
                    }
                } catch (Exception ignored) {
//                    System.out.println(ignored.getMessage());
                }
            }
            iterator.close();
            dataStore.dispose();
//            System.out.println(id);
            return id;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }


    public static int readGeomFromDBSearch(DataStore dataStore, String tableName, HashMap<String, SimpleFeature> shpMap, List<Entry<Integer, Geometry>> entryList) {
        try {
            SimpleFeatureSource source = dataStore.getFeatureSource(tableName);
            Filter filter = Filter.INCLUDE; // this is an empty filter - fetches all features
            Query query = new Query(tableName, filter);
            SimpleFeatureCollection features = source.getFeatures(query);
            if (features == null) return 0;
            //读取数据
            SimpleFeatureIterator iterator = features.features();
            int id = 0;

            while (iterator.hasNext()) {
                SimpleFeature feature = iterator.next();
                try {
                    org.locationtech.jts.geom.Geometry geomCollection = (org.locationtech.jts.geom.Geometry) feature.getDefaultGeometryProperty().getValue();

                    for (int i = 0; i < geomCollection.getNumGeometries(); i++) {
                        org.locationtech.jts.geom.Geometry geom = geomCollection.getGeometryN(i);
                        if (geom.getCoordinates().length < 1) continue;
                        com.github.davidmoten.rtree.geometry.Geometry rtCol = null;
                        if (geom instanceof LineString) {
                            LineString pro = (LineString) geom;
                            rtCol = Tools.getGeoCod(pro.getBoundary());
                        } else if (geom instanceof Polygon) {
                            Polygon pro = (Polygon) geom;
                            rtCol = Tools.getGeoCod(pro.getBoundary());
                        }
                        assert rtCol != null;
                        if (!shpMap.containsKey(rtCol.toString())) {
                            shpMap.put(rtCol.toString(), feature);
                            entryList.set((Integer) feature.getAttribute("id") - 1, entry(id, rtCol));
                            id++;
                        }
                    }
                } catch (Exception ignored) {
//                    System.out.println(ignored.getMessage());
                }
            }
            iterator.close();
            dataStore.dispose();
            System.out.println(id);
            return id;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static void writeGeomToDB(DataStore dataStore, String saveTable, List<Future<ThreeTuple<Integer, List, List<SimpleFeature>>>> futures, Class<? extends org.locationtech.jts.geom.Geometry> geomClass, String keyString, String pubKey, String outputFile, int count, HashMap<String, List<Integer>> rtreeIndexLeaf, List<String> keyList) throws IOException {
        try {
            SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
            builder.setName(saveTable);
            builder.setCRS(null); // <- Coordinate reference system; null means default
            builder.add("id", Integer.class);
            builder.add("geom", geomClass);
            SimpleFeatureType schema = builder.buildFeatureType();

            // Check if the schema exists
            String typeName = schema.getTypeName();
            if (Arrays.asList(dataStore.getTypeNames()).contains(typeName)) {
                // If the schema exists, remove it
                dataStore.removeSchema(typeName);
            }
            dataStore.createSchema(schema);

            Integer[][] recordList = new Integer[futures.size()][3];
            List<SimpleFeature> sortList = new ArrayList<>(Collections.nCopies(count, null));
            int futureCount = 0;
            List<Integer> test = new ArrayList<>();
            for (Future<ThreeTuple<Integer, List, List<SimpleFeature>>> future : futures) {
                ThreeTuple<Integer, List, List<SimpleFeature>> tt = future.get();
                for (int i = 0; i < tt.second.size(); i++) {
                    sortList.set((Integer) tt.second.get(i), tt.third.get(i));
                }
                recordList[futureCount] = new Integer[]{tt.third.size(), tt.first};
                rtreeIndexLeaf.put(keyList.get(futureCount), tt.second);
                futureCount += 1;
            }

            FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriterAppend(schema.getTypeName(), Transaction.AUTO_COMMIT);
            // 写记录
            int id = 1;
            for (SimpleFeature future : sortList) {
                try {
                    SimpleFeature fNew = writer.next();
                    fNew.setAttribute("id", id++);
                    fNew.setAttribute("geom", future.getAttribute("geom"));
//                    System.out.println(future.getAttribute("geom"));
                    writer.write();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            SimpleFeatureSource source = dataStore.getFeatureSource(saveTable);

            Filter filter = Filter.INCLUDE; // this is an empty filter - fetches all features
            Query query = new Query(saveTable, filter);
            SimpleFeatureCollection features = source.getFeatures(query);
            //读取数据
            SimpleFeatureIterator iterator = features.features();
            List testList = new ArrayList();
            while (iterator.hasNext()) {
                SimpleFeature feature = iterator.next();
                testList.add(feature.getAttributes());
            }
            iterator.close();

            TwoTuple<Integer[][], String> recodeTwo = new TwoTuple<>(recordList, keyString);
            byte[] serializeRecode = SerializeUtils.serialize(recodeTwo);
            String encString = SM2Utils.encrypt(SecurityUtils.hexStringToBytes(pubKey), serializeRecode);
            try {
                String outputKey = outputFile.substring(0, outputFile.lastIndexOf(".")) + ".key";
                File file1 = new File(outputKey);
                FileWriter osPub = new FileWriter(file1, false);
                BufferedWriter osPubbw = new BufferedWriter(osPub);
                osPubbw.write(encString);
                osPubbw.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            writer.close();
            dataStore.dispose();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void saveRTree(String outputFile, RTree<Integer, Geometry> tree) throws IOException {
        OutputStream os = new FileOutputStream(outputFile + "rtreeStar");
        Serializer<Integer, Geometry> serializer = Serializers.flatBuffers().javaIo();
        serializer.write(tree, os);
    }


    public static void main(String[] args) throws Exception {

    }
}