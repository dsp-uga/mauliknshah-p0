package mauliknshah.p0

/**
  * @author Maulik Shah
  * @version 1.0
  */

import org.apache.spark.sql._
import java.io._

import org.apache.commons.io._





object SPAll {
  /**
    * This method convers a (Word,Count) list into a Json file and stores it into the
    * given Location.
    * @param filteredWordList list of  (word,count)
    * @param fileName File Name and location to create the JSON file.
    */
  def saveInJSON(filteredWordList: List[(String,Int)], fileName : String): Unit ={
    var jsonOutput: String = "{"; //Prefix Character for JSON file.
    var counter = 0;//List Counter.

    //For all the word,count pairs, create a row for the JSON file.
    for(wordRow <- filteredWordList){
      counter = counter + 1
      jsonOutput = jsonOutput.concat( "\"" + wordRow._1 + "\":" + wordRow._2)

      if(counter != filteredWordList.size){
        jsonOutput = jsonOutput.concat(",\n") //Add the comma and new line IFF it's not the last pair.
      }//end if.
    }//end for.

    jsonOutput = jsonOutput.concat("}")
    try{
      FileUtils.writeStringToFile(new File(fileName),jsonOutput)//Print to the file.
    }catch{
      case e: Exception => e.printStackTrace()
    }

  }


  /**
    * This method converts a (Word,tfIDF) list into a Json file and stores it into the
    * given Location.
    * @param filteredWordList list of  (word,tfIDF)
    * @param fileName File Name and location to create the JSON file.
    * OverRidden method.
    */
  def saveInJSONDouble(filteredWordList: List[(String,Double)], fileName : String): Unit ={
    var jsonOutput: String = "{"; //Prefix Character for JSON file.
    var counter = 0;//List Counter.

    //For all the word,count pairs, create a row for the JSON file.
    for(wordRow <- filteredWordList){
      counter = counter + 1
      jsonOutput = jsonOutput.concat( "\"" + wordRow._1 + "\":" + wordRow._2)

      if(counter != filteredWordList.size){
        jsonOutput = jsonOutput.concat(",\n") //Add the comma and new line IFF it's not the last pair.
      }//end if.
    }//end for.

    jsonOutput = jsonOutput.concat("}")
    try{
      FileUtils.writeStringToFile(new File(fileName),jsonOutput)//Print to the file.
    }catch{
      case e: Exception => e.printStackTrace()
    }

  }




  //Main method, which reads all the files.
  def main(args: Array[String]) {

      //Create Spark Session and context.
      val sparkSession = SparkSession.builder.appName("SP1").config("spark.master","local").getOrCreate()
      val spark = sparkSession.sparkContext


//      //Get the file
      var textFile = spark.textFile("data")
//
//      //Devide the text file into words, with converting everything into lowercase(case-insensitive).
//      //Cache this word list.
      textFile = textFile.map(_.toLowerCase)
      val textWords = textFile.flatMap(_.split(" ")).cache()
//     //textWords.collect().foreach(println)
//
//      //SP1
//      //Reduce the list by the word count.
      val wordCounter = textWords.map(word => (word,1)).reduceByKey((c1,c2) => c1+c2)
////      wordCounter.collect().foreach(println)
//
//      //Filter the word count. Remove the words having count lower than 2 and pick the first 40 words.
      val wordCountFiltered = wordCounter.filter(word => word._2 > 2).filter(word =>word._1.size>0).sortBy(_._2,false)
//      //wordCountFiltered.collect().foreach(println)
//
//     //Find the final 40 words, which is the actual List.
      val finalWords = spark.parallelize(wordCountFiltered.take(40))
//    //      finalWords.collect().foreach(println)
//
//      //Convert the Word into a list, so that it can be used to create a custom
//      //JSON file using String concat.
      val finalWordsList : List[(String,Int)] = finalWords.collect().toList
//
//      //Save to a JSON File.
      saveInJSON(finalWordsList, "out/sp1.json")
//
//      //SP2
//      //Get the list of the stop words from the file and convert it into a set. The list is case insensitive,so in lower case.
      val stopWords = spark.textFile("stopwords/stopwords.txt").map(_.toLowerCase())
      val stopWordsSet = stopWords.collect().toSet
//
//      //Filtered List by removing the stopwords and take the top 40 again. Save to the sp2 JSON file.
      val stopWordFiltered = spark.parallelize(wordCountFiltered.filter(word => !stopWordsSet.contains(word._1)).take(40))
      val wordsWithoutStopWords : List[(String,Int)] = stopWordFiltered.collect().toList
      saveInJSON(wordsWithoutStopWords, "out/sp2.json")
//
//       // SP3
//      //Remove the Punctuation marks from the words from starting and ending positions.
//      //Start from the basic text words.
//      //Default
      val textPuncFiltered = textWords.map(_.replaceAll("^[.,:;'!?]+|[.,:;'!?]+$",""))
//       //Reduce the list by the word count.
      val puncWordCounter = textPuncFiltered.map(word => (word,1)).reduceByKey((c1,c2) => c1+c2)
//
//      //Filter the punctuation removed word count. Apply the logic of A and B again.
//      // Remove the words having count lower than 2 and pick the first 40 words and also remove the stop words.
//      // Create a list and Save.
      val puncWCFiltered = puncWordCounter.filter(word => word._2 > 2).filter(word =>word._1.size>1).sortBy(_._2,false)
      val puncWCSWFiltered = puncWCFiltered.filter(word => !stopWordsSet.contains(word._1))
      val puncWCSFReduced = spark.parallelize(puncWCSWFiltered.take(40))
      val wordsWithoutPuncSW : List[(String,Int)] = puncWCSFReduced.collect().toList
      saveInJSON(wordsWithoutPuncSW, "out/sp3.json")



      //SP4
      //Get the file with case insensitive and punctuation removed data.
      val tfIdfFiles = spark.wholeTextFiles("data").map( text => (text._1,text._2.toLowerCase())).cache()

     //Null List of the word, tf-idf list.
      var tfIdfAll : List[(String,String,Double)] = List[(String,String,Double)]()

      //Overall Document count.
      val docCount = tfIdfFiles.count()

      //Remember to  Strip white spaces.


      //Remove Punctuation, Stop words and words with size less than 1.
      val tokenized = tfIdfFiles.map( texts => (texts._1,
                                                    texts._2.split(" ").map(_.replaceAll("^[.,:;'!?]+|[.,:;'!?]+$",""))
                                                      .filter(_.size > 1).filter(word => !stopWordsSet.contains(word))))
//      println("Tokenized:")
//      print(tokenized.count())
      //Now, you have term frequency for each document-word.
      //Remove the words with counts less than 2.
      val tf = tokenized.flatMapValues(text => text).countByValue().filter(text => text._2 > 2)
//    println("TF:")
//    print(tf.size)

      //Find the document frequency
      //Convert Words as a key and the document name as a value
      //Calculate the number of occurance for each word in the documents.
      val df = tokenized.flatMapValues(text => text).distinct().map(word => (word._2,word._1)).countByKey()
//    println("DF:")
//    print(df.size)

//       var count = 1
      //Now Calculate TF-IDF for each document-word.
      for(tfWord <- tf){
//          count = count + 1;

//        if (count%50 == 0){
//          println(count)
//        }


        //Get the Filtered Word.
//        val dfFiltered = df.filter(_._1.equals(tfWord._1._2))
          val dfVal = df.get(tfWord._1._2)

        //If the DF is filtered or not.
        if(dfVal != null){
             //DF Value.

             //TF-IDF Value
             val tfIdf = (tfWord._2 * math.log(docCount.toDouble/(dfVal.get))).round.toDouble

             if(tfIdf != 0){
               //Append the List.
               tfIdfAll = tfIdfAll :+ (tfWord._1._1,tfWord._1._2,tfIdf)
             }//end if.

          }//end if.
      }//end for.
    val tfIDFRDD = spark.parallelize(tfIdfAll)
    var topList : List[(String,Double)] = List[(String, Double)]()
    for(file <- tfIdfFiles.collect().toList){

        val topListForDoc: List[(String,Double)]= spark.parallelize(tfIDFRDD.filter(text => text._1.equals(file._1))
                                                  .map(text => (text._2,text._3))
                                                    .sortBy(_._2,false).take(5)).collect().toList
        topList = topList ::: topListForDoc
    }

    saveInJSONDouble(topList, "out/sp4.json")


      //end Spark Session.
      spark.stop()
  }//end method.
}//end class.
