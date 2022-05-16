package mongo.crud.mongodb_transaction.test;

import com.mongodb.client.*;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;

import java.util.Arrays;

@TestConfiguration
@SpringBootTest
public class AggregateTests {


    private static MongoClient mongoClient = null;
    private static MongoCollection<Document> collection = null;

    private static ClientSession clientSession = null;

    static {
        // DB 연결
        mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase database = mongoClient.getDatabase("test");
        collection = database.getCollection("aggregation");
    }

    // Test data
    /*db.aggregation.insertMany(
        [
            { "_id" : 'agg1', "name" : "park", "score" : 50, "subject" : 'math' },
            { "_id" : 'agg2', "name" : "kim", "score" : 60, "subject" : 'science' },
            { "_id" : 'agg3', "name" : "cha", "score" : 65, "subject" : 'math' },
            { "_id" : 'agg4', "name" : "an", "score" : 100, "subject" : 'math' },
            { "_id" : 'agg5', "name" : "lee", "score" : 30, "subject" : 'science' },
            { "_id" : 'agg6', "name" : "choi", "score" : 74, "subject" : 'math' },
            { "_id" : 'agg7', "name" : "park", "score" : 75, "subject" : 'science' }
        ]
    );*/

    @Test
    public void testMatch() {
        // $match - 조건에 만족하는 Document만 Filtering
        // db.aggregation.aggregate( [ { $match : { name : "park" } } ] );

        // given
        // Test data

        // when
        AggregateIterable<Document> documents = collection.aggregate(Arrays.asList(new Document("$match", new Document("name", "park"))));

        documents.forEach(s -> System.out.println("JSON >>> " + s));

        // then
        documents.forEach(s -> Assertions.assertEquals(s.get("name"), "park"));
    }

    @Test
    public void testGroup() {
        // $group - Document에 대한 Grouping 연산, Group에 대한 id를 지정해야함, 정렬 지원 X
        // db.aggregation.aggregate( [ { $group : { _id : "$subject", avg : { $avg : "$score" }, count : { $sum : 1 } } } ] );

        // given
        // Test data

        // when
        AggregateIterable<Document> documents = collection.aggregate(
                Arrays.asList(
                        new Document("$group"
                                , new Document("_id", "$subject")
                                        .append("avg", new Document("$avg", "$score"))
                                        .append("count", new Document("$sum", 1)))));

        documents.forEach(s -> System.out.println("JSON >>> " + s));

        // then
        Document document = documents.first(); // 순서 X
        if (document.get("_id").equals("science")) {
            Assertions.assertEquals(document.get("avg"), 55.0);
            Assertions.assertEquals(document.get("count"), 3);
        } else {
            Assertions.assertEquals(document.get("avg"), 72.25);
            Assertions.assertEquals(document.get("count"), 4);
        }
    }
}
