package modeling

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object DataModeling {
  def run(spark: SparkSession, df: DataFrame): Unit = {
    import spark.implicits._

    // -------------------------------------------------------
    // Build vertices: unique station list with stationId
    // -------------------------------------------------------
    println("=== Vertex Creating ===")
    println("Assigning a unique ID to Each Station...")
    val stations = df.select("City", "Location")
      .distinct()
      .withColumn("stationId", monotonically_increasing_id())
      .cache()

    println("Converting to RDD for GraphX...")
    val vertices: RDD[(VertexId, (String, String))] =
      stations.rdd.map(row => (row.getAs[Long]("stationId"), (row.getAs[String]("City"), row.getAs[String]("Location"))))


    println("Sample vertices (stationId, (City, Location)):")
    vertices.take(5).foreach(println)

    // -------------------------------------------------------------------
    // Compute average pollution (avg_global_index) per station
    // -------------------------------------------------------------------
    println("=== Edge (connection) Creating ===")
    println("Compute average pollution (avg_global_index) per station")
    val stationPollution = df.groupBy("City", "Location")
      .agg(avg("global_index").alias("avg_global_index"))

    println("Sample avg_global_index by station:")
    stationPollution.show(5, truncate = false)

    // -------------------------------------------------------------------
    // Build edges between stations whose avg_global_index are close
    // -------------------------------------------------------------------

    // Step 1: consider optimized strategies (blocking, approximate NN).
    val stationPairs = stationPollution.as("a").crossJoin(stationPollution.as("b"))
      .filter( ($"a.City" =!= $"b.City") || ($"a.Location" =!= $"b.Location") )
      .withColumn("diff", abs($"a.avg_global_index" - $"b.avg_global_index"))
      .filter($"diff" < 0.2)
      .select(
        $"a.City".alias("City1"), $"a.Location".alias("Location1"),
        $"b.City".alias("City2"), $"b.Location".alias("Location2"),
        $"diff"
      )

    // Step 2: Join to get srcId, dstId
    val edgesDF = stationPairs
      .join(stations.withColumnRenamed("City", "City1").withColumnRenamed("Location", "Location1"), Seq("City1", "Location1"))
      .withColumnRenamed("stationId", "srcId")
      .join(stations.withColumnRenamed("City", "City2").withColumnRenamed("Location", "Location2"), Seq("City2", "Location2"))
      .withColumnRenamed("stationId", "dstId")
      .select("srcId", "dstId", "diff")
      .na.drop()

    edgesDF.show()

    println(s"\nTotal rows: ${edgesDF.count()}")
    println(s"Total columns: ${edgesDF.columns.length}")
    println("Columns: " + edgesDF.columns.mkString(", "))

    // -------------------------------------------------------------------
    // Convert to RDD[Edge[Double]]
    // -------------------------------------------------------------------

    // Step 1: Convert to GraphX Edge RDD and remove duplicates/self-loops
    val edgesRDD = edgesDF.rdd.map(row => (row.getAs[Long]("srcId"), row.getAs[Long]("dstId"), row.getAs[Double]("diff")))
      .filter { case (s, d, _) => s != d }
      .map { case (s, d, diff) => if (s < d) (s, d, diff) else (d, s, diff) }
      .map { case (s, d, diff) => ((s, d), diff) }
      .reduceByKey((a, b) => math.min(a, b))
      .map { case ((s, d), diff) => Edge(s, d, diff) }
      .cache()

    // vertices is RDD[(VertexId, (String, String))]
    println(vertices.getClass)

    vertices.take(1).foreach(println)
    // Example output: (0, (Paris, Center))


    edgesRDD.take(1).foreach(println)
    // Example output: Edge(0, 1, 1.0)

    // Step 2: Build Graph
    println("=== Edge (connection) Creating ==")
    val graph = Graph(vertices, edgesRDD)
    println(s"Graph has ${graph.numVertices} vertices and ${graph.numEdges} edges")


    // ---------------------------------------------------------------------------------------------
    // Analyze Graph - Example: Degree (number of connections per station)
    // ---------------------------------------------------------------------------------------------
    println("=== Analyze Graph ==")
    val degrees = graph.degrees.collect()
    // Step 1: Station degrees (connections)
    println("Station degrees (connections):")
    degrees.foreach { case (id, deg) =>
      val station = vertices.filter(_._1 == id).first()
      println(s"${station._2._1} - ${station._2._2}: $deg connections")
    }

    // Step 2: PageRank (pollution spread potential)
    val ranks = graph.pageRank(0.0001).vertices.collect()
    println("PageRank (spread potential) per station:")
    ranks.sortBy(-_._2).take(10).foreach { case (id, rank) =>
      val station = vertices.filter(_._1 == id).first()
      println(f"${station._2._1} - ${station._2._2}: PageRank=$rank%.6f")
    }
  }
}
