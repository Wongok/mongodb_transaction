package mongo.crud.mongodb_transaction.test;

import com.mongodb.client.*;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

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

    @Test
    public void testProject() {
        // $project - Project에서 지정한 필드 값을 다음 파이프라인 단계로 전달, 0: 필드 미표시 1: 필드 표시
        // db.aggregation.aggregate( [ { $project : { score : 1, subject : 1 } } ] );

        // given
        // Test data

        // when
        AggregateIterable<Document> documents = collection.aggregate(
                Arrays.asList(
                        new Document("$project"
                                , new Document("score", 1)
                                .append("subject", 1))));

        documents.forEach(s -> System.out.println("JSON >>> " + s));

        documents.forEach(s -> Assertions.assertFalse(s.containsKey("name")));
    }

    @Test
    public void testSort() {
        // $sort - 정렬 조건에 맞게 파이프라인의 연산결과를 정렬, 1: ASC -1: DESC
        // db.aggregation.aggregate( { $sort : { score : -1 } } );

        // given
        // Test data

        // when
        AggregateIterable<Document> documents = collection.aggregate(
                Arrays.asList(
                        new Document("$sort", new Document("score", -1))));

        documents.forEach(s -> System.out.println("JSON >>> " + s));

        // then
        Assertions.assertEquals(documents.first().get("score"), 100.0);
    }

    @Test
    public void testSkip() {
        // $skip - 입력한 갯수만큼 차례대로 Document를 skip 한 데이터를 다음 파이프라인에 전달
        // db.aggregation.aggregate( [ { $skip : 4 } ] );

        // given
        // Test data

        // when
        AggregateIterable<Document> documents = collection.aggregate(Arrays.asList(new Document("$skip", 4)));

        documents.forEach(s -> System.out.println("JSON >>> " + s));

        // then
        Assertions.assertEquals(documents.first().get("_id"), "agg5");
    }

    @Test
    public void testSample() {
        // $sample - Collection 내에서 입력한 갯수만큼 랜덤하게 Document 출력
        // db.aggregation.aggregate( [ { $sample : { size : 4 } } ] );

        // given
        // Test data

        // when
        AggregateIterable<Document> documents = collection.aggregate(Arrays.asList(new Document("$sample", new Document("size", 4))));

        AtomicInteger cnt = new AtomicInteger();
        documents.forEach(s -> System.out.println("JSON " + cnt.incrementAndGet() + " >>>" + s));

        // then
        Assertions.assertEquals(cnt.get(), 4);
    }

    @Test
    public void testCount() {
        // $count - Document의 카운트를 다음 단계로 전달
        // db.aggregation.aggregate( [ { $match : { score : { $gt : 70 } } }, { $count : "70_over_cnt" } ] );

        // given
        // Test data

        // when
        AggregateIterable<Document> documents = collection.aggregate(
                Arrays.asList(
                        new Document("$match"
                                , new Document("score", new Document("$gt", 70)))
                        , new Document("$count", "70_over_cnt")));

        System.out.println("JSON >>> " + documents.first());

        // then
        Assertions.assertEquals(documents.first().get("70_over_cnt"), 3);
    }

    @Test
    public void testAddFields() {
        // $addFields - Document에 새 필드 추가, 실제 Document 문서 내용 변경 X, 조회 목적
        // db.aggregation.aggregate( [ { $addFields : { "grade" : 1 } } ] );

        // given
        // Test data

        // when
        AggregateIterable<Document> documents = collection.aggregate(Arrays.asList(new Document("$addFields", new Document("grade", 1))));

        documents.forEach(s -> System.out.println("JSON >>> " + s));

        // then
        Assertions.assertEquals(documents.first().get("grade"), 1);
    }

    @Test
    public void testUnwind() {
        // $unwind - Document내의 배열 필드를 기반으로 각각의 Document로 분리
        // db.aggregation2.aggregate( [ { $match : { name : "separk" } }, { $unwind : "$subjects" } ] );

        // given
        // { "_id" : agg8, "name" : "separk", "subject" : [ "math", "science", "korean" ] };

        // when
        AggregateIterable<Document> documents = collection.aggregate(
                Arrays.asList(
                        new Document("$match"
                                , new Document("name", "separk"))
                        , new Document("$unwind", "$subjects")));

        documents.forEach(s -> System.out.println("JSON >>> " + s));

        // then
        Assertions.assertEquals(documents.first().get("subjects"), "math");
    }
}
