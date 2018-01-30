import com.mongodb.*;
import com.mongodb.util.JSON;
//import com.sun.tools.internal.xjc.reader.xmlschema.bindinfo.BIConversion;
//import com.mongodb.ObjectId;

public class MongoDB {

    public static void main(String[] args) {
        MongoClient mongoClient = new MongoClient("10.1.3.51",27017);

        DB psdoc = mongoClient.getDB("zootest");
        DBCollection user=psdoc.getCollection("ICHUB_INVOKE_LOG");


//        DBObject dbObject = (DBObject) JSON.parse("2");
//        //插入数据库
//        user.insert(dbObject);

        DBCursor cursor=user.find();
        while(cursor.hasNext())
        {
            DBObject obj=cursor.next();

            System.out.println(obj.toString());
        }
    }
}
