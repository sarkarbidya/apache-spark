
package org.apache.spark.examples.sql.studentSystem

import org.apache.spark.sql.SecureSparkSession
import org.apache.spark.sql.LabeledRow
import org.apache.spark.sql.types._



object StudentSystemMain{
  def main(args: Array[String]): Unit = {
    val secureSpark = SecureSparkSession.getSecureSparkSession("18111011","password")
    import secureSpark.implicits._
   


//    getAllDetails(secureSpark)
    getIndivisualDetail(secureSpark)
    //getAllGradeForCourse(secureSpark)
  }


  private def getAllDetails(secureSpark: SecureSparkSession) :  Unit ={// working fine as nothing is displayed

    try{
      getAllStudentDetails(secureSpark)
      getAllFacultyDetails(secureSpark)
      getAllCourseDetails(secureSpark)
      getAllGradeDetails(secureSpark)
    }catch {
      case e: UnsupportedOperationException => println("illegal access request")
    }

  }


  private def getIndivisualDetail(secureSpark: SecureSparkSession) : Unit ={
    try{
      getOwnStudentDetail(secureSpark) // working fine displayed single row
      getOtherStudentDetail(secureSpark,18111012) // working fine nthing displayed
      getFacultyDetail(secureSpark,"ABC") // nothing displayed
      getCourseDetail(secureSpark,"CS628") // nothing displayed
      getOwnGradeForSingleCourse(secureSpark,"CS628") // nothong displayed as owner subset check failed 
      getOwnGradeForAllCourse(secureSpark) // displayed nothing as owner subset check failed
    }catch {
      case e:UnsupportedOperationException =>  println("illegal access request")
    }
    
  }

  /**
   *
   * 
   * throws UnsupportedOperationException when label mismatch is found
   */

  private def getAllStudentDetails(secureSpark: SecureSparkSession) : Unit = {
    val secureDF = secureSpark.secureRead.json("examples/src/main/scala/org/apache/spark/examples/sql/studentSystem/Files/student.json")
    secureDF.show()
  }


  /**
   *
   * 
   * @throws UnsupportedOperationException when label mismatch is found
   */

  private def getAllFacultyDetails(secureSpark: SecureSparkSession) : Unit = {
    val secureDF = secureSpark.secureRead.json("examples/src/main/scala/org/apache/spark/examples/sql/studentSystem/Files/faculty.json")
    secureDF.show()
  }


  /**
   *
   * 
   * @throws UnsupportedOperationException when label mismatch is found
   */

  private def getAllCourseDetails(secureSpark: SecureSparkSession) : Unit = {
    val secureDF = secureSpark.secureRead.json("examples/src/main/scala/org/apache/spark/examples/sql/studentSystem/Files/course.json")
    secureDF.show()
  }



  /**
   *
   * 
   * @throws UnsupportedOperationException when label mismatch is found
   */

  private def getAllGradeDetails(secureSpark: SecureSparkSession) : Unit = {
    val secureDF = secureSpark.secureRead.json("examples/src/main/scala/org/apache/spark/examples/sql/studentSystem/Files/grade.json")
    secureDF.show()
  }


  /**
   *
   * 
   * @throws UnsupportedOperationException when label mismatch is found
   */

  private def getOwnStudentDetail(secureSpark: SecureSparkSession) : Unit = {

    import secureSpark.implicits._
    val secureDF = secureSpark.secureRead.json("examples/src/main/scala/org/apache/spark/examples/sql/studentSystem/Files/student.json")
    secureDF.filter($"rollNo" === secureSpark.userName).show()

  }


  /**
   *
   * 
   * @throws UnsupportedOperationException when label mismatch is found
   */

  private def getOtherStudentDetail(secureSpark: SecureSparkSession,roll: Long) : Unit = {

    import secureSpark.implicits._
    val secureDF = secureSpark.secureRead.json("examples/src/main/scala/org/apache/spark/examples/sql/studentSystem/Files/student.json")
    secureDF.filter($"rollNo" === roll).show()

  }


  /**
   *
   * 
   * @throws UnsupportedOperationException when label mismatch is found
   */

  private def getFacultyDetail(secureSpark: SecureSparkSession, name: String) : Unit = {
    val secureDF = secureSpark.secureRead.json("examples/src/main/scala/org/apache/spark/examples/sql/studentSystem/Files/faculty.json")
    import secureSpark.implicits._
    secureDF.filter($"name" === name).show()

  }



  /**
   *
   * 
   * @throws UnsupportedOperationException when label mismatch is found
   */

  private def getCourseDetail(secureSpark: SecureSparkSession, courseName: String) : Unit = {
    val secureDF = secureSpark.secureRead.json("examples/src/main/scala/org/apache/spark/examples/sql/studentSystem/Files/course.json")
    import secureSpark.implicits._
    secureDF.filter($"instructor" === courseName).show()

  }


  /**
   *
   * 
   * @throws UnsupportedOperationException when label mismatch is found
   */


  private def getOwnGradeForSingleCourse(secureSpark: SecureSparkSession, courseID: String) : Unit = {
    val secureDF = secureSpark.secureRead.json("examples/src/main/scala/org/apache/spark/examples/sql/studentSystem/Files/grade.json")
    
  
    import secureSpark.implicits._
                              
    secureDF.filter($"courseID" === courseID && $"rollNo" === secureSpark.userName).show()

  }


  /**
   *
   * 
   * @throws UnsupportedOperationException when label mismatch is found
   */

  private def getOwnGradeForAllCourse(secureSpark: SecureSparkSession) : Unit = {
    val secureDF = secureSpark.secureRead.json("examples/src/main/scala/org/apache/spark/examples/sql/studentSystem/Files/grade.json")
    
  
    import secureSpark.implicits._
                              
    secureDF.filter($"rollNo" === secureSpark.userName).show()

  }



  /**
   *
   * 
   * @throws UnsupportedOperationException when label mismatch is found
   */

  private def getAllGradeForCourse(secureSpark: SecureSparkSession) : Unit = {
    

    val secureDFGrade = secureSpark.secureRead.json("examples/src/main/scala/org/apache/spark/examples/sql/studentSystem/Files/grade.json")
    
    val secureDFCourse = secureSpark.secureRead.json("examples/src/main/scala/org/apache/spark/examples/sql/studentSystem/Files/course.json")
    
    import secureSpark.implicits._
    
    val courseID : String = secureDFCourse.secureSelect("courseID","instructor")
                              .filter($"instructor" === secureSpark.userName)
                              .first().getString(0)
                              
    println("course found "+courseID)
    secureDFGrade.show()
    secureDFGrade.filter($"courseID" === courseID).show()

  }
}
