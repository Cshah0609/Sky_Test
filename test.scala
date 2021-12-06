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
    val env_val =env
    val date_val = date
    for(i <- 0 until a.length ){
      sparkSession.sql( ${a(i)})

    }
    
    //Below are the queries getting executed :
    sparkSession.sql (s"""Select count( *) from (
      select channel_code, count(*) from channel_table_${env_val}
       group by channel_code
      having count*) > 1)""".stripMargin)

    sparkSession.sql(
      s"""
         |select count (*)
         |from
         |channel_transaction_${env_val} A,
         |channel_table_${env_val} B
         |left join on (A.channel_code= B.channel_code)
         |where B.channel_code is null
         |and B.transaction_date =${date_val}
         |""".stripMargin)

    sparkSession.sql(
      s"""
         |select count (*) from
         |channel_transaction_${env_val}
         |where transaction_date = ${date_val}
         |and transaction_amount
         |is null
         |""".stripMargin)



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
