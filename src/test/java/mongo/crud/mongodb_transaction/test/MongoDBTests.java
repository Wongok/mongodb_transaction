package mongo.crud.mongodb_transaction.test;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;
import java.util.Map;

@SpringBootConfiguration
@SpringBootTest
public class MongoDBTests {

    private static MongoCollection<Document> collection = null;

    static {
        // DB 연결
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase database = mongoClient.getDatabase("test");
        collection = database.getCollection("collection1");
    }

    @Test
    public void testFind() {
        // given
        Document document = collection.find(Filters.eq("name", "separk")).first();
        System.out.println("JSON >> " + document.toJson());

        // then
        Assertions.assertEquals(document.get("age"), 30.0);
    }

    @Test
    public void testInsert() {
        // given
        Map<String, Object> documentMap = new HashMap<String, Object>();
        documentMap.put("name", "new_separk");
        documentMap.put("age", 99);

        collection.insertOne(new Document(documentMap));

        // when
        Document document = collection.find(Filters.eq("name", "new_separk")).first();
        System.out.println("JSON >> " + document.toJson());

        // then
        Assertions.assertEquals(document.get("age"), 99);
    }

    @Test
    public void testUpdate() {
        // given
        Map<String, Object> documentMap = new HashMap<String, Object>();
        documentMap.put("name", "new_separk");
        documentMap.put("age", 88);

        collection.updateOne(Filters.eq("name", "new_separk"), new BasicDBObject("$set", documentMap));

        // when
        Document document = collection.find(Filters.eq("name", "new_separk")).first();
        System.out.println("JSON >> " + document.toJson());

        // then
        Assertions.assertEquals(document.get("age"), 88);
    }

//    @Test
//    public void testStartTransaction() {
//
//    }
//
//    @Test
//    public void testCommitTransaction() {
//
//    }
//
//    @Test
//    public void testAbortTransaction() {
//
//    }
//
//    @Test
//    public void testWithTransaction () {
//
//    }

}
