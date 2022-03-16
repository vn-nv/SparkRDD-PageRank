import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object PageRank {
    def main(args: Array[String]) {
        val input_dir = "sample_input"
        val links_file = input_dir + "/links-simple-sorted.txt"
        val titles_file = input_dir + "/titles-sorted.txt"
        val num_partitions = 10
        val iters = 10

        val conf = new SparkConf()
            .setAppName("PageRank")
            .setMaster("local[*]")
            .set("spark.driver.memory", "1g")
            .set("spark.executor.memory", "2g")

        val sc = new SparkContext(conf)
        
        val links_rdd = sc
            .textFile(links_file, num_partitions)
            
        val links = links_rdd
            .map{ x => (x.split(": ")(0), x.split(":")(1).split("\\s+").tail) }

        val titles_rdd = sc
            .textFile(titles_file, num_partitions)

        val N = titles_rdd.count()

        val pages_rdd = titles_rdd
            .zipWithIndex()
            .map{x => ((x._2.toInt + 1).toString, x._1)}

        var ranks_0 = pages_rdd.mapValues(v => 100.0 / N)

        /* PageRank */
        for (i <- 1 to iters) {
                val contribs = ranks_0.join(links).values.flatMap{
                    case(rank, urls) => 
                    val size = urls.size
                    urls.map(link=>(link, rank/size))
                }
                var ranks_n = contribs.reduceByKey(_ + _).mapValues(15.0 / N + 0.85 * _)
                ranks_0 = ranks_n ++ ranks_0.subtractByKey(ranks_n).mapValues(x => (15.00 / N))
        }

        println("[ PageRanks ]")

        val sum = ranks_0.map(_._2).sum()

        val rank = ranks_0
            .mapValues{x => x * 100 / sum}

        val page_rank = pages_rdd
            .join(rank)
            .takeOrdered(10)(Ordering[Double].reverse.on(x => x._2._2))
            .foreach(println)
    
    }
}
