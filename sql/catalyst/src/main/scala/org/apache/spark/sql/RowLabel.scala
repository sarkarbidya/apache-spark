
package org.apache.spark.sql

import scala.collection.mutable.HashMap

abstract class Label

case class Owner(ow : String) extends Label

case class Reader(rdr : Set[String]) extends Label

case class RowLabel(rowLabel : HashMap[Owner, Reader] ) extends Label{
  def isRestriction(label : HashMap[Owner, Reader]) : Boolean = {
      var rowOwnerSet = rowLabel.keySet
      var userOwnerSet = label.keySet
      var isValid : Boolean = true
     
      println("row label in restriction " +rowLabel)
      println("label in restriction "+label)

  
      if(rowOwnerSet.subsetOf(userOwnerSet)){
        println("row label owner validated")
        for(owner <- rowOwnerSet){
          val rowReader = rowLabel.get(owner).get
          val userReader = label.get(owner).get
              
          val rowReaderSet = rowReader.rdr
          val userReaderSet = userReader.rdr
        
          if(! userReaderSet.subsetOf(rowReaderSet) ){
            isValid = false
          
          }
          else
            isValid = true
          
        }
        isValid
      }else{
        isValid = false
        isValid
      }
  }


  def doUnion(tableLabel : HashMap[Owner,Reader]) : HashMap[Owner,Reader] = {
    var tableOwnerSet = tableLabel.keySet
    var labelOwnerSet = rowLabel.keySet
    var ownerSet = tableOwnerSet.union(labelOwnerSet)
    var schemaLabel : HashMap[Owner,Reader] = HashMap.empty[Owner,Reader]
    for(owner <- ownerSet) {
      var readerSet :  Set[String] = Set()
      var readerSet1:  Set[String] = Set()
      var readerSet2: Set[String] = Set()

      if(tableLabel.contains(owner)){
        readerSet1 = tableLabel.get(owner).get.rdr
      }

      if(rowLabel.contains(owner) ){
        readerSet2 = rowLabel.get(owner).get.rdr
      }
      if(  (! tableLabel.isEmpty ) && (! readerSet1.isEmpty && ! readerSet2.isEmpty)  ) {
        readerSet = readerSet1.intersect(readerSet2)
      } else{
        readerSet = readerSet1.union(readerSet2)
      }
      schemaLabel += (owner -> Reader(readerSet))
    }
    println("dounion label " + schemaLabel)
    schemaLabel
  }
}

