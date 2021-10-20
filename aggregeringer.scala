import java.io._
import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import org.mongodb.scala._
import org.mongodb.scala.model._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model.UpdateOptions
import org.mongodb.scala.bson.BsonObjectId


object MongoGAggr extends App {
    implicit class DocumentObservable[C](val observable: Observable[Document]) extends ImplicitObservable[Document] {
        override val converter: (Document) => String = (doc) => doc.toJson
    }

    implicit class GenericObservable[C](val observable: Observable[C]) extends ImplicitObservable[C] {
        override val converter: (C) => String = (doc) => doc.toString
    }

    trait ImplicitObservable[C] {
        val observable: Observable[C]
        val converter: (C) => String

        def results(): Seq[C] = Await.result(observable.toFuture(), Duration(10, TimeUnit.SECONDS))
        def headResult() = Await.result(observable.head(), Duration(10, TimeUnit.SECONDS))
        def printResults(initial: String = ""): Unit = {
        if (initial.length > 0) print(initial)
        results().foreach(res => println(converter(res)))
        }
        def printHeadResult(initial: String = ""): Unit = println(s"${initial}${converter(headResult())}")
    }

    val mongoClient: MongoClient = MongoClient();
    val database: MongoDatabase = mongoClient.getDatabase("gruppe2");
    val collection: MongoCollection[Document] = database.getCollection("happiness");

    println("sorted by score")
    collection.aggregate(Seq(Aggregates.sort(orderBy(descending("score"))),
                             Aggregates.limit(10))).printResults()

    println("sorted by gdp")
    collection.aggregate(Seq(Aggregates.sort(orderBy(descending("gdp"))),
                             Aggregates.limit(10))).printResults()
    //db.happiness.aggregate(         [             {$sort : { score : -1 }}         , {$limit : 10 } ]     )
    

}