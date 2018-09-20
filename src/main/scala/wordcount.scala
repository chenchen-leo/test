
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._


object wordcount {
  def main(args: Array[String]): Unit = {

    //创建sc
    val spark=SparkSession.builder().master("local").enableHiveSupport().getOrCreate()
//    spark.conf.set("es.nodes","localhost")
//    spark.conf.set("es.port","9200")
    val sc=spark.sparkContext
//    val data=sc.esRDD("test/usr")

    val readoptions = Map( "es.nodes" -> "192.168.1.233","es.port" -> "9200","es.net.http.auth.user"-> "elastic","es.net.http.auth.pass"->"changeme")
    val writeoptions = Map("es.index.auto.create" -> "true", "es.nodes" -> "192.168.1.233","es.port" -> "9200","es.net.http.auth.user"-> "elastic","es.net.http.auth.pass"->"changeme")

    val inputdf = spark.read.format("es").options(readoptions).load("test/usr")
    val dfarray=inputdf.collect()

    for (i<-0 until dfarray.size){
      val dfmap=scala.collection.mutable.Map[Any,Any]()
      for (j<-0 until inputdf.schema.length){
        dfmap+=(inputdf.schema(j).name -> dfarray(i)(j))
      }
      sc.makeRDD(Seq(dfmap)).saveToEs("write/test",writeoptions)
    }

    spark.stop()

    //     ./spark-submit --class cn.cas.test.tableAug jar包 <input file>

  }
}
