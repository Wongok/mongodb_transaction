# MongoDB Replica Set Local Setting in Windows

## Replication
> MongoDB에서 레플리케이션(Replication)이란 고가용성(High-Availability)을 위해서 데이터베이스를 복제하는 것을 뜻한다. 여기서 고가용성은 데이터베이스에 항상 이용 가능한 상태를 추구하는 것이고, 데이터베이스를 복제한다는 것은 데이터베이스 내의 내용물을 다른 MongoDB 인스턴스에 복제한다는 것이다. 

<p align="center"><img src="https://user-images.githubusercontent.com/37280323/168470283-bda1e946-7fd2-4907-a553-57188718c67f.png"></p>

1. Primary, Secondary를 나눌 디렉토리 및 로그파일을 생성한다.

> Primary : 메인 서버 / 읽기 쓰기 모두 가능  
> Secondary : 복제 서버 / 읽기만 가능  
> Arbiter : 모니터링 서버

2. mongod.cfg 파일 수정
```shell
replication:
  replSetName: "rs0" 
```

3. 각각의 CMD에 mongod 실행
```shell
mongod --dbpath "C:\data\primary\dbpath" --logpath "C:\data\primary\log.log" --replSet "rs0" --port 27017
mongod --dbpath "C:\data\secondary-1\dbpath" --logpath "C:\data\secondary-1\log.log" --replSet "rs0" --port 27027
mongod --dbpath "C:\data\secondary-2\dbpath" --logpath "C:\data\secondary-2\log.log" --replSet "rs0" --port 27037
```
replSet 이름은 같게 / dbpath, logpath, port는 다르게 설정

4. 새로운 CMD에서 replSet 설정
```shell
mongo --port 27017

rs.initiate({
    "_id" : "rs0",
    "members" :[
     { "_id" : 0, host : "127.0.0.1:27017"},
     { "_id" : 1, host : "127.0.0.1:27027"},
     { "_id" : 2, host : "127.0.0.1:27037"}
     ]
});
```

설정 중 아래와 같은 에러 발생 시 shutdown 후 재실행
```shell
{
        "ok" : 0,
        "errmsg" : "This node was not started with the replSet option",
        "code" : 76,
        "codeName" : "NoReplicationEnabled" 
}
```
```shell
mongo --eval "db.getSiblingDB('admin').shutdownServer()" 
```

참고URL : https://koonkim.gitbook.io/mongodb/