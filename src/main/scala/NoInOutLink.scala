import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object NoInOutLink {
    def main(args: Array[String]) {
        val input_dir = "sample_input"
        val links_file = input_dir + "/links-larger-sorted.txt"
        val titles_file = input_dir + "/titles-larger-sorted.txt"
        val num_partitions = 10
        

        val conf = new SparkConf()
            .setAppName("NoInOutLink")
            .setMaster("local[*]")
            .set("spark.driver.memory", "1g")
            .set("spark.executor.memory", "2g")

        val sc = new SparkContext(conf)

        val links_rdd = sc
            .textFile(links_file, num_partitions)
            
        val outlinks_rdd = links_rdd
            .map( _.split(": ")(0) )
            .map( index => (index.toInt, "title") )

        val titles_rdd = sc
            .textFile(titles_file, num_partitions)
            
        val pages_rdd = titles_rdd
            .zipWithIndex()
            .map{
                x => (x._2.toInt + 1, x._1)
            }

        val inlinks_rdd = links_rdd
            .map( _.split(": ")(1) )
            .flatMap( _.split("\\s+"))
            .distinct()
            .map( index => (index.toInt, "title") )
        /* No Outlinks */
        
        println("[ NO OUTLINKS ]")
        var no_outlinks = pages_rdd
            .subtractByKey(outlinks_rdd)
            .takeOrdered(10)(Ordering[Int].on(x => x._1))
            .foreach(println)

        /* No Inlinks */
        println("\n[ NO INLINKS ]")
        val no_inlinks = pages_rdd
            .subtractByKey(inlinks_rdd)
            .takeOrdered(10)(Ordering[Int].on(x => x._1))
            .foreach(println)
    }
}

