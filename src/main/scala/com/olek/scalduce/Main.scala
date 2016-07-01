package com.olek.scalduce


object Main {

  // Example 1: Count words and sort desc by frequency
  def countWords( implicit inputData : List[(String, String)] ) = {

    implicit val countedWords = MapReduce.executeJob[String, Int, (String,  Int)](
      (x , y ) =>  y.split("\\s+").map{ v => ( v , 1)  },
      (a  , b ) => (a, b.foldRight(0)( _ + _)),
      (a , b) => a._1.compareTo( b._1 ) > 0

    )

    MapReduce.executeJob[Int, Int, (String, Int)](
      (x , y ) => Seq((x, y)) ,
      (x , y ) => (x, y.head) ,
      (x, y ) => x._2.compareTo( y._2) > 0)

  }

  // Example 2: Inverted index
  def buildInvertedIndex( implicit inputData : List[(String, String)] ) = {

    MapReduce.executeJob[String, String,  (String, Seq[String])](
      (x , y ) =>  y.split("\\s+").map{ v => ( v , x)  },
      (a  , b ) => (a, b.toSet.toList.sorted  ),
      (a , b) => a._1.compareTo( b._1 ) > 0
    )

  }

  // Example 3: Grep
  def grep( searchWord : String)(implicit inputData : List[(String, String)] ) = {

    MapReduce.executeJob[String, String, (String, Seq[String])](
      (x , y ) =>  if (y.matches(".*" + searchWord + ".*")) Seq((x, y)) else Nil ,
      (a  , b ) => (a, b ),
      (a , b) => a._1.compareTo( b._1 ) > 0
    )
  }

  // Example 4: Longest sentences
  def longestSentences(implicit inputData : List[(String, String)] ) = {

    MapReduce.executeJob[String, Int, String](
      (x , y ) =>  Seq((y, y length)) ,
      (a  , b ) => a,
      (a , b) => {
          val c = b._2.compareTo( a._2 )
          if (c == 0) {
            a._1.compareTo( b._1 ) < 0
          } else {
            c < 0
          }
      }
    )
  }

  // Example 5: To upper case
  def toUpperCase(implicit inputData : List[(String, String)] ) = {

    MapReduce.executeJob[String, String,  (String, String)](
      (x , y ) =>  Seq((x, y toUpperCase)),
      (a  , b ) => (a, b.head ),
      (a , b) => a._1.compareTo( b._1 ) < 0
    )
  }


  // Example 6: Common friends
  def commonFriends(implicit inputData : Seq[(String, Seq[String])] ) = {

    MapReduce.executeJob[Seq[String], Set[String],  (Seq[String], Seq[String])](
      (x , y ) =>  (for( i <- y ) yield ( List(x, i).sorted.mkString(","), y.toSet)),
      (a  , b ) => ( a.split(",").toList, b.reduce( _ intersect _).toSeq.sorted.toList ),
      (a , b) => a._1.compareTo( b._1 ) < 0
    )
  }

  def print[A]( i : String, s : Seq[A] ) = {
    println( i + ": " + (s mkString "; ") )
  }



  def main(args : Array[String]) : Unit = {

    implicit val inputData = List( ("tweet1", "hello world"),
      ("tweet2", "hello kitty"),
      ("tweet3","hello map reduce"),
      ("tweet4","map reduce job"),
      ("tweet5", "already hello done job"),
      ("tweet6", "map your map to the map")
    )

    print( "Word Count", countWords )

    print( "Inverted Index",  buildInvertedIndex)

    print( "Grep",  grep( "map") )

    print( "Longest Sentence", longestSentences )

    print( "To Upper Case", toUpperCase )

    implicit val inputFriends = Map(
      "A" -> List( "B", "C", "D" ),
      "B" -> List( "A", "C", "D", "E" ),
      "C" -> List( "A", "B", "D", "E", "F" ),
      "D" -> List( "A", "B", "C", "E" ),
      "E" -> List( "B", "C", "D" ),
      "F" -> List( "C" )
    ).toSeq

    print( "Common Friends", commonFriends )

  }

}
