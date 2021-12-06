package test

import org.apache.spark.sql.SparkSession

object test {

  def readTable(sparkSession: SparkSession, tableName: String) :List[String] = {
    val df = sparkSession.sql(s"Select * from  ${tableName}")
      .select("test_sql")
      .map(f => f.getString(0))
      .collect()
      .toList

    df

  }

  def executeQaChecks( qaCheckSql : List[String], env :String, date : String,sparkSession : SparkSession) = {

    val a = qaCheckSql
    for(i <- 0 until a.length ){
      sparkSession.sql( ${a(i)})
       }

  }

  def main(args: Array[String]): Unit = {
    val env = "DEV"
    val date = "2021-12-06"
    val sparkSession = SparkSession.builder.enableHiveSupport.getOrCreate()
    val tableName = "qa_tests"
    //Read table and get the QA Check Test Sqls.
    val qachecks_sql = readTable(sparkSession, tableName)

    //Execute the Test SQL one by one.
    executeQaChecks(qachecks_sql,env,date,sparkSession)


  }


}
