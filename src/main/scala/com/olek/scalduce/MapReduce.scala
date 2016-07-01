package com.olek.scalduce


object MapReduce {



  def partitionBy[T, B](list: Seq[T], f : T => B ) : List[Seq[T]] = {
      if (!list.isEmpty) {
          val h = list.head
          val segment = list takeWhile {f(h) == f(_)}
          segment :: partitionBy(list drop segment.length, f)
      } else {
          Nil
      }
  }



  def executeJob[B, A, C]( mapf : ( String, B ) => Seq[(String, A)] ,
                 reducef : (String , Seq[A]) => C ,
                 sortf : ((String, A), (String, A))=> Boolean )
                         (implicit coll : Seq[( String , B)]) : Seq[C] = {

    val afterMapBeforeShufflingAndReduce =  coll.flatMap{ case ( x , y ) => mapf( x , y) }.sortWith(sortf)

    partitionBy[(String, A), String]( afterMapBeforeShufflingAndReduce  , _._1 ).map{ y =>  reducef( y.head._1 , y.map{ _._2 }  )     }

  }

}
