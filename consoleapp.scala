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

object MongoConsoleApp extends App {
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
    val collection: MongoCollection[Document] = database.getCollection("consoleapp");


    var stopped = false
    println("hey there, using this program you can register users to the mongodb database")
    var command = 0
    while(!stopped){
        println("type 1 to add a new user, 2 to view the users in the database, 3 to sort by income"
     + " and 4 to quit the program")
        command = readInt()
        if(command==1){
            val name = readLine("Enter the name of the user: ")
            val country = readLine("Enter the country of the user: ")
            print("Enter the yearly income of the user: ")
            val income = readInt()
            print("Enter the age of the user: ")
            val age = readInt()

            val document = Document(
            "name" -> name, 
            "country" -> country,
            "income" -> income,
            "age" -> age 
            );

            collection.insertOne(document).results();
            println("added user " + name)

        }else if(command==2){
            collection.find(Document()).printResults()

        }else if(command==3){
             println("sorted by income")
             collection.aggregate(Seq(Aggregates.sort(orderBy(descending("income"))))).printResults()
        }
        else if(command == 4){
            stopped = true
            println("quiting program....")
        }else{
            println("you typed a wrong command, try again")
           
        }
        
    }
    
}