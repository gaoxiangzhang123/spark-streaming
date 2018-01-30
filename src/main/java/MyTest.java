//import com.mongodb.MongoClient;
//import com.mongodb.MongoClientURI;
//import com.mongodb.client.FindIterable;
//import com.mongodb.client.MongoCollection;
//import com.mongodb.client.MongoDatabase;
//import org.apache.commons.io.FileUtils;
//import org.bson.Document;
//
//import java.io.File;
//import java.io.IOException;
//import java.nio.charset.Charset;
//import java.util.ArrayList;
//import java.util.List;
//
//
///**
// * Created by Administrator on 2018/1/5.
// */
//public class MyTest {
//    public static void main(String[] args) throws IOException {
////		String uri = args[0];
////		String database = args[1];
//
//    String uri = "mongodb://root:Gingkoo123!@dds-uf6b4f42666a7b542.mongodb.rds.aliyuncs.com:3717";
//    String database = "zoo";
//
//    MongoClient mongoClient = new MongoClient(new MongoClientURI(uri));
//    MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
//
//    File idFile = new File("/Users/kane/Documents/temp/111111/ori.txt");
//
//    try {
//        List<String> ids = FileUtils.readLines(idFile, Charset.forName("UTF-8"));
//        int count = 0;
//        List<String> realIds = new ArrayList<>();
//        for (String id : ids) {
//            count++;
//            logger.info("getting [{}] {}", count, id);
//            MongoCollection<Document> collection = mongoDatabase.getCollection("ICHUB_INVOKE_LOG");
//            String realId = id.replace("ICHUB_INVOKE_LOG,", "").trim();
//            realIds.add(realId);
//            if (count % 1000 == 0 || count == ids.size()) {
//                logger.info("search begin");
//                FindIterable<Document> documents = collection.find(new Document("__ID__", new Document("$in", realIds)));
//                logger.info("search end");
//
//                int index = 0;
//                List<String> lines = new ArrayList<>();
//                for (Document document : documents) {
//                    index++;
//                    logger.info("iterator index [{}]", index);
//                    lines.add(document.toJson());
//                }
//                File resultFile = new File("/Users/kane/Documents/temp/111111/result/result-" + count + ".txt");
//                logger.info("writing begin");
//                FileUtils.writeLines(resultFile, lines, "UTF-8", true);
//                logger.info("writing end");
//                realIds = new ArrayList<>();
//            }
//        }
//
//    } catch (IOException e) {
//        logger.error("出错啦", e);
//
//    }
//}
//}