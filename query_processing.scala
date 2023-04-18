









import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf



    val inputFile = "/home/dinesh/spark/facebook_combined.txt"
   

    // Load the edges as a graph
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, inputFile)

    var continue = true

    while (continue) {

      // Define the switch case options
      println(s"Choose an option:")
      println(s"1. Number of vertices")
      println(s"2. Number of edges")
      println(s"3. Neighbors of a vertex")
      println(s"4. Number of triangles in the graph")
      println(s"5. Top k vertices with max degree")
      println(s"6. Shortest path between two vertices")
      println(s"7. Connected components")
      println(s"8. Exit")

      // Prompt user for input
      print("Enter an option number: ")
      val option = scala.io.StdIn.readInt()

      // Switch case to handle different options
      option match {
        case 1 =>
          println(s"Number of vertices: ${graph.vertices.count()}")
        case 2 =>
          println(s"Number of edges: ${graph.edges.count()}")
        case 3 =>
          val vertexId = scala.io.StdIn.readLong()
          val neighbors = graph.edges.filter(_.srcId == vertexId).map(_.dstId).collect()
          println(s"Neighbors of $vertexId: $neighbors")
        case 4 =>
          val triangleCountGraph = graph.triangleCount()
          val numTriangles = triangleCountGraph.vertices.map{ case (vid, count) => count }.reduce(_ + _) / 3
          println(s"Number of triangles in the graph: $numTriangles")
        case 5 =>
          val k = scala.io.StdIn.readInt()
          val topKVertices = graph.degrees.top(k)(Ordering.by(_._2))
          topKVertices.foreach(println)
        case 6 =>
             val src: VertexId = 0L
    val dst: VertexId = 15L
    val initialGraph = graph.mapVertices((id, _) => if (id == src) 0.0 else Double.PositiveInfinity)
    // send the message along the edges to update the distances
    val messages = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // update function
      triplet => {  // send message function
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // merge function
    )
    // get the shortest path
    val shortestPath = messages.vertices.filter(_._1 == dst).first()._2
    println(s"Shortest path between $src and $dst: $shortestPath")
        case 7 =>
          val cc = graph.connectedComponents().vertices.collectAsMap()
          cc.foreach{
            case (vertexId, clusterId) =>
              println(s"Vertex $vertexId belongs to cluster $clusterId")
          }
        case 8 =>
          println("Exiting")
          case _ =>
println("Invalid option. Please choose a valid option.")
}
}


