package mongo.crud.mongodb_transaction.test;

import com.mongodb.*;
import com.mongodb.client.*;
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

    private static MongoClient mongoClient = null;
    private static MongoCollection<Document> collection = null;

    private static ClientSession clientSession = null;

    static {
        // DB 연결
        mongoClient = MongoClients.create("mongodb://localhost:27017");
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

    @Test
    public void testStartTransaction() {
        // step 1 : start a client session
        clientSession = mongoClient.startSession();

        try {
            collection = mongoClient.getDatabase("test").getCollection("collection1");
            clientSession.startTransaction();

            Map<String, Object> documentMap = new HashMap<String, Object>();
            documentMap.put("name", "col1_separk");
            documentMap.put("age", 12);

            collection.updateOne(Filters.eq("name", "col1_separk"), new BasicDBObject("$set", documentMap));

        } catch (Exception e) {
            e.printStackTrace();
            clientSession.abortTransaction();

        } finally {
            clientSession.close();  // commitTransaction이 없어도 커밋됨

            Document document = collection.find(Filters.eq("name", "col1_separk")).first();
            System.out.println("JSON >> " + document.toJson());

            // then
            Assertions.assertEquals(document.get("age"), 12);
        }
    }

    @Test
    public void testCommitTransaction() {

    }

    @Test
    public void testAbortTransaction() {
        // clientSession = mongoClient.startSession();
        clientSession = mongoClient.startSession(ClientSessionOptions.builder()
                .defaultTransactionOptions(TransactionOptions.builder()
                        .readPreference(ReadPreference.primary())
                        .writeConcern(WriteConcern.W1)
                        .readConcern(ReadConcern.LOCAL)
                        .build())
                .build());

        try {
            collection = mongoClient.getDatabase("test").getCollection("collection1");
            clientSession.startTransaction();

            Map<String, Object> documentMap1 = new HashMap<String, Object>();
            documentMap1.put("name", "col1_abort1");
            documentMap1.put("age", 33);

            Map<String, Object> documentMap2 = new HashMap<String, Object>();
            documentMap2.put("name", "col1_abort2");
            documentMap2.put("age", 22);

            collection.updateOne(Filters.eq("name", "col1_abort1"), new BasicDBObject("$set", documentMap1));
            Integer.parseInt("a");  // 에러발생
            collection.updateOne(Filters.eq("name", "col1_abort2"), new BasicDBObject("$set", documentMap2));

        } catch (Exception e) {
            e.printStackTrace();
            clientSession.abortTransaction();

        } finally {
            clientSession.close();

            Document document = collection.find(Filters.eq("name", "col1_abort1")).first();
            System.out.println("JSON >> " + document.toJson());

            // then
            Assertions.assertEquals(document.get("age"), 33);
        }
    }

    @Test
    public void testWithTransaction () {
        // step 1 : start a client session
        clientSession = mongoClient.startSession();

        // step 2 : Optional. Define options to use for the transation.
        TransactionOptions options = TransactionOptions.builder()
                .readPreference(ReadPreference.primary())
                .readConcern(ReadConcern.LOCAL)
                .writeConcern(WriteConcern.MAJORITY)
                .build();

        // step 3 : Define the sequence of operations to perform inside the transactions.
        TransactionBody<String> body = new TransactionBody<String>() {
            @Override
            public String execute() {
                MongoCollection<Document> collection1 = mongoClient.getDatabase("test").getCollection("collection1");
                MongoCollection<Document> collection2 = mongoClient.getDatabase("test").getCollection("collection2");

                Map<String, Object> documentMap1 = new HashMap<String, Object>();
                documentMap1.put("name", "col1_separk");
                documentMap1.put("age", 1);

                Map<String, Object> documentMap2 = new HashMap<String, Object>();
                documentMap2.put("name", "col2_separk");
                documentMap2.put("age", 2);

                collection1.insertOne(clientSession, new Document(documentMap1));
                collection2.insertOne(clientSession, new Document(documentMap2));
                return "Success";
            }
        };

        try {
            // step 4 : Use .withTransaction() to start a transaction, execute the callback, and commit (or abort on error)
            clientSession.withTransaction(body, options);
            System.out.println("transaction : " + clientSession.hasActiveTransaction());
            clientSession.commitTransaction();

        } catch (Exception e) {
            e.printStackTrace();
            clientSession.abortTransaction();

        } finally {
            clientSession.close();

            MongoCollection<Document> collection1 = mongoClient.getDatabase("test").getCollection("collection1");
            Document document = collection1.find(Filters.eq("name", "col1_separk")).first();
            System.out.println("JSON >> " + document.toJson());

            // then
            Assertions.assertEquals(document.get("age"), 1);
        }
    }

}
