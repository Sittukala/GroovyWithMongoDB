package lab2

import java.util.regex.Pattern

import org.bson.BSON
import org.bson.Document

import com.mongodb.BasicDBList
import com.mongodb.BasicDBObject
import com.mongodb.BasicDBObjectBuilder
import com.mongodb.client.MongoClients
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.UpdateOptions

import com.mongodb.client.internal.MongoClientImpl

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

import com.mongodb.Block;
import com.mongodb.DBObject
import com.mongodb.client.FindIterable
import com.mongodb.client.MongoClient;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;

import com.mongodb.client.model.Aggregates.*;
def USERNAME = 'Sittukala'
def PWD = 'MongoDB'
def DATABASE = 'test-co7217'

// MAKING THE CONNECTION
def mongoClient = MongoClients.create("mongodb+srv://Sittukala:MongoDB@cluster0-nmjac.mongodb.net/test?retryWrites=true&w=majority");

// GET DATABASE
def db = mongoClient.getDatabase(DATABASE);

//int Total=db.getCollection("climate_change").countDocuments()
//println "total tweets" +Total


final String parameter = args[0]

println "argument"+parameter


if(parameter=="ex01") {
	JsonSlurper jsonSlurper=new JsonSlurper()
	climatedata=jsonSlurper.parse(new File('src/main/resources/climate_change_tweets_all.json'))
	//println(climatedata)
	db.createCollection("climate_change")
	
	def col=db.getCollection("climate_change")
	for (obj in climatedata) {
	   
	   def doc = Document.parse(JsonOutput.toJson(obj))
	   col.insertOne(doc)
	}
}

else if(parameter=="ex02")
{

//Q1.

int Total=db.getCollection("climate_change").countDocuments()
println "total tweets" +Total

def key = "full_text"

def Capitalism_regex="\\bcapitalism\b"
def Trump_regex="\\bTrump\b"
def Obama_regex="\\bObama\b"
def USA_regex="\\bUSA\\b"
def UK_regex="\\bUK\\b"
def Carbon_regex="\\bcarbon\\b"
def Sealevel_regex="\\bsea level\\b"
def Temperature_regex="\\btemperature\\b"
def Rainfall_regex = "\\brainfall\\b"


def Capitalism_count=0
def Trump_count=0
def Obama_count=0
def USA_count=0
def UK_count=0
def Carbon_count=0
def Sealevel_count=0
def Temperature_count=0
def Rainfall_count=0

def regex2 = "\\bcapitalism\\b"
def new_count2=0
for ( def v in db.getCollection("climate_change").find(BasicDBObjectBuilder.start().add(key, Pattern.compile(regex2)).get()))
{

   new_count2++
}
def Capitalism_percent=((new_count2/Total)*100)
println "Capitalism: " + new_count2+ "  ("+ Capitalism_percent  + "%)"

def regex1 = "\\bTrump\\b"
def new_count1=0
for ( def v in db.getCollection("climate_change").find(BasicDBObjectBuilder.start().add(key, Pattern.compile(regex1)).get()))
{

   new_count1++
}
def Trump_percent=((new_count1/Total)*100)
println "Trump: " + new_count1 + "  ("+ Trump_percent  + "%)"

def regex = "\\bObama\\b"
def new_count=0
for ( def v in db.getCollection("climate_change").find(BasicDBObjectBuilder.start().add(key, Pattern.compile(regex)).get()))
{

   new_count++
}
def Obama_percent=((new_count/Total)*100)
println "Obama: " + new_count + "  ("+ Obama_percent  + "%)"


for ( def v in db.getCollection("climate_change").find(BasicDBObjectBuilder.start().add(key, Pattern.compile(USA_regex)).get()))
{

  USA_count++
}
def US_percent=((USA_count/Total)*100)
println "USA: " + USA_count + "  ("+ US_percent  + "%)"

for ( def v in db.getCollection("climate_change").find(BasicDBObjectBuilder.start().add(key, Pattern.compile(UK_regex)).get()))
{

  UK_count++
}
def UK_percent=((UK_count/Total)*100)
println "UK: " + UK_count + "  ("+ UK_percent  + "%)"

for ( def v in db.getCollection("climate_change").find(BasicDBObjectBuilder.start().add(key, Pattern.compile(Sealevel_regex)).get()))
{

  Sealevel_count++
}
def Sealevel_percent=((Sealevel_count/Total)*100)
println "Sea level: " + Sealevel_count + "  ("+ Sealevel_percent  + "%)"

for ( def v in db.getCollection("climate_change").find(BasicDBObjectBuilder.start().add(key, Pattern.compile(Carbon_regex)).get()))
	{
	
	  Carbon_count++
	}
	def Carbon_percent=((Carbon_count/Total)*100)
	println "Carbon: " + Carbon_count + "  ("+ Carbon_percent  + "%)"
	
	
	
	for ( def v in db.getCollection("climate_change").find(BasicDBObjectBuilder.start().add(key, Pattern.compile(Temperature_regex)).get()))
	{
	
	  Temperature_count++
	}
	def Temperature_percent=((Temperature_count/Total)*100)
	println "Temperature: " + Temperature_count + "  ("+ Temperature_percent  + "%)"
	for ( def v in db.getCollection("climate_change").find(BasicDBObjectBuilder.start().add(key, Pattern.compile(Rainfall_regex)).get()))
	{
	
	  Rainfall_count++
	}
	def Rainfall_percent=((Rainfall_count/Total)*100)
	println "Rainfall: " + Rainfall_count + "  ("+ Rainfall_percent  + "%)"
}

else if(parameter=="ex03_a"){
	for (def i in  db.getCollection("climate_change").find().sort(new BasicDBObject("favorite_count",-1)).limit(5))
		{
		
		
		println  "tweet " + i.id + "  ( favorite Count " + i.favorite_count + ")  " + "\n";
		text=i.id;
		println i.full_text + "\n";
		}
}
else if(parameter=="ex03_b"){
	def initial_fulltext="";
	int counter=0
	for (def i in  db.getCollection("climate_change").find(Filters.gt("retweet_count", 10000)).sort(Sorts.descending("retweet_count")).projection(Projections.fields(Projections.include("id", "full_text", "retweet_count"), Projections.excludeId())))
		{
			
		if(initial_fulltext.equals(i.full_text)) {
				continue;
			}
				else {
			
			initial_fulltext=i.full_text;
		
		println  "tweet " + i.id + "  ( retweet Count " + i.retweet_count + ")  " + "\n";
		
		println "\t" + i.full_text + "\n";
		println "-----------------------------------"
				}
				
		counter++
			if(counter==5)
				break;
		}
}
else if(parameter=="ex03_c"){
		for (def i in  db.getCollection("climate_change").find().sort(new BasicDBObject("user.followers_count",-1)).limit(5))
			{
			
		/*
			println  "tweet " + i.id + "  ( fowllowers Count " + i.user["followers_count"] + ")  " + "\n";
			
			println i.full_text + "\n";*/
				
			println "user" + " " + i.user["id"] + "-" + " " +i.user["name"]+":"+ "fowllowers Count " + "(" +i.user["followers_count"] +")"
			}
	
}





