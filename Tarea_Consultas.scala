// Databricks notebook source
//Convertir a parquet
val clickstream = sqlContext
    .read
    .format("csv")
    .options(Map("header" -> "true", "delimiter" -> "\t", "mode" -> "PERMISSIVE", "inferSchema" -> "true"))
    .load("dbfs:///databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed")

clickstream.show(5)

// Convert the DatFrame to a more efficent format to speed up our analysis
clickstream
  .write
  .mode(SaveMode.Overwrite)
  .parquet("/datasets/wiki-clickstream") // warnings are harmless

// COMMAND ----------

val data = sc.textFile("/datasets/wiki-clickstream/part-00006-tid-1570432442797041251-2bd06ec5-29cf-4283-ad47-b15b9166053e-278-1-c000.snappy.parquet")

// COMMAND ----------

val clickstream = sqlContext.read.parquet("/datasets/wiki-clickstream/part-00006-tid-1570432442797041251-2bd06ec5-29cf-4283-ad47-b15b9166053e-278-1-c000.snappy.parquet")

// COMMAND ----------

clickstream.printSchema

// COMMAND ----------

display(clickstream)

// COMMAND ----------

clickstream.count()

// COMMAND ----------

display(clickstream
        .select(clickstream("curr_title"), clickstream("prev_title"), clickstream("n"))
        .filter("prev_title = 'other-google'")
        .groupBy("curr_title").sum()
        .orderBy($"sum(n)".desc)
        .limit(10))

// COMMAND ----------

display(clickstream
        .select(clickstream("curr_title"), clickstream("prev_title"), clickstream("n"))
        .filter("curr_title = 'Twitter'")
        .limit(10))

// COMMAND ----------

display(clickstream
        .select(clickstream("curr_title"), clickstream("n"))
        .filter("curr_title = 'Enrique_Peña_Nieto'")
        .groupBy("curr_title").sum()
        )

// COMMAND ----------

clickstream.createOrReplaceTempView("clicks_table")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *
// MAGIC   FROM clicks_table
// MAGIC   WHERE 
// MAGIC     curr_title = 'The_Weeknd' AND
// MAGIC     prev_id IS NOT NULL AND prev_title != 'Main_Page'
// MAGIC   ORDER BY n DESC
// MAGIC   LIMIT 20

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT 
// MAGIC       prev_title,
// MAGIC       curr_title,
// MAGIC       n
// MAGIC     FROM clicks_table
// MAGIC     WHERE 
// MAGIC       curr_title IN ('J_Balvin', 'Bad_Bunny', 'Daddy_Yankee', 'Farruko') AND
// MAGIC       prev_id IS NOT NULL AND prev_title != 'Main_Page'
// MAGIC     ORDER BY n DESC
// MAGIC     LIMIT 20

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *
// MAGIC   FROM clicks_table
// MAGIC   WHERE 
// MAGIC     curr_title = 'Tesla' AND
// MAGIC     prev_id IS NOT NULL AND prev_title != 'Main_Page'
// MAGIC   ORDER BY n DESC
// MAGIC   LIMIT 10

// COMMAND ----------

// MAGIC %scala
// MAGIC package d3
// MAGIC  // We use a package object so that we can define top level classes like Edge that need to be used in other cells
// MAGIC //Este código fue desarrollado por "Michael Armbrust at Spark Summit East February 2016"
// MAGIC  
// MAGIC  import org.apache.spark.sql._
// MAGIC  import com.databricks.backend.daemon.driver.EnhancedRDDFunctions.displayHTML
// MAGIC  
// MAGIC  case class Edge(src: String, dest: String, count: Long)
// MAGIC  
// MAGIC  case class Node(name: String)
// MAGIC  case class Link(source: Int, target: Int, value: Long)
// MAGIC  case class Graph(nodes: Seq[Node], links: Seq[Link])
// MAGIC  
// MAGIC  object graphs {
// MAGIC  val sqlContext = SQLContext.getOrCreate(org.apache.spark.SparkContext.getOrCreate())  
// MAGIC  import sqlContext.implicits._
// MAGIC    
// MAGIC  def force(clicks: Dataset[Edge], height: Int = 100, width: Int = 960): Unit = {
// MAGIC    val data = clicks.collect()
// MAGIC    val nodes = (data.map(_.src) ++ data.map(_.dest)).map(_.replaceAll("_", " ")).toSet.toSeq.map(Node)
// MAGIC    val links = data.map { t =>
// MAGIC      Link(nodes.indexWhere(_.name == t.src.replaceAll("_", " ")), nodes.indexWhere(_.name == t.dest.replaceAll("_", " ")), t.count / 20 + 1)
// MAGIC    }
// MAGIC    showGraph(height, width, Seq(Graph(nodes, links)).toDF().toJSON.first())
// MAGIC  }
// MAGIC  
// MAGIC  /**
// MAGIC   * Displays a force directed graph using d3
// MAGIC   * input: {"nodes": [{"name": "..."}], "links": [{"source": 1, "target": 2, "value": 0}]}
// MAGIC   */
// MAGIC  def showGraph(height: Int, width: Int, graph: String): Unit = {
// MAGIC  
// MAGIC  displayHTML(s"""
// MAGIC  <!DOCTYPE html>
// MAGIC  <html>
// MAGIC  <head>
// MAGIC    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
// MAGIC    <title>Polish Books Themes - an Interactive Map</title>
// MAGIC    <meta charset="utf-8">
// MAGIC  <style>
// MAGIC  
// MAGIC  .node_circle {
// MAGIC    stroke: #777;
// MAGIC    stroke-width: 1.3px;
// MAGIC  }
// MAGIC  
// MAGIC  .node_label {
// MAGIC    pointer-events: none;
// MAGIC  }
// MAGIC  
// MAGIC  .link {
// MAGIC    stroke: #777;
// MAGIC    stroke-opacity: .2;
// MAGIC  }
// MAGIC  
// MAGIC  .node_count {
// MAGIC    stroke: #777;
// MAGIC    stroke-width: 1.0px;
// MAGIC    fill: #999;
// MAGIC  }
// MAGIC  
// MAGIC  text.legend {
// MAGIC    font-family: Verdana;
// MAGIC    font-size: 13px;
// MAGIC    fill: #000;
// MAGIC  }
// MAGIC  
// MAGIC  .node text {
// MAGIC    font-family: "Helvetica Neue","Helvetica","Arial",sans-serif;
// MAGIC    font-size: 17px;
// MAGIC    font-weight: 200;
// MAGIC  }
// MAGIC  
// MAGIC  </style>
// MAGIC  </head>
// MAGIC  
// MAGIC  <body>
// MAGIC  <script src="//d3js.org/d3.v3.min.js"></script>
// MAGIC  <script>
// MAGIC  
// MAGIC  var graph = $graph;
// MAGIC  
// MAGIC  var width = $width,
// MAGIC      height = $height;
// MAGIC  
// MAGIC  var color = d3.scale.category20();
// MAGIC  
// MAGIC  var force = d3.layout.force()
// MAGIC      .charge(-700)
// MAGIC      .linkDistance(180)
// MAGIC      .size([width, height]);
// MAGIC  
// MAGIC  var svg = d3.select("body").append("svg")
// MAGIC      .attr("width", width)
// MAGIC      .attr("height", height);
// MAGIC      
// MAGIC  force
// MAGIC      .nodes(graph.nodes)
// MAGIC      .links(graph.links)
// MAGIC      .start();
// MAGIC  
// MAGIC  var link = svg.selectAll(".link")
// MAGIC      .data(graph.links)
// MAGIC      .enter().append("line")
// MAGIC      .attr("class", "link")
// MAGIC      .style("stroke-width", function(d) { return Math.sqrt(d.value); });
// MAGIC  
// MAGIC  var node = svg.selectAll(".node")
// MAGIC      .data(graph.nodes)
// MAGIC      .enter().append("g")
// MAGIC      .attr("class", "node")
// MAGIC      .call(force.drag);
// MAGIC  
// MAGIC  node.append("circle")
// MAGIC      .attr("r", 10)
// MAGIC      .style("fill", function (d) {
// MAGIC      if (d.name.startsWith("other")) { return color(1); } else { return color(2); };
// MAGIC  })
// MAGIC  
// MAGIC  node.append("text")
// MAGIC        .attr("dx", 10)
// MAGIC        .attr("dy", ".35em")
// MAGIC        .text(function(d) { return d.name });
// MAGIC        
// MAGIC  //Now we are giving the SVGs co-ordinates - the force layout is generating the co-ordinates which this code is using to update the attributes of the SVG elements
// MAGIC  force.on("tick", function () {
// MAGIC      link.attr("x1", function (d) {
// MAGIC          return d.source.x;
// MAGIC      })
// MAGIC          .attr("y1", function (d) {
// MAGIC          return d.source.y;
// MAGIC      })
// MAGIC          .attr("x2", function (d) {
// MAGIC          return d.target.x;
// MAGIC      })
// MAGIC          .attr("y2", function (d) {
// MAGIC          return d.target.y;
// MAGIC      });
// MAGIC      d3.selectAll("circle").attr("cx", function (d) {
// MAGIC          return d.x;
// MAGIC      })
// MAGIC          .attr("cy", function (d) {
// MAGIC          return d.y;
// MAGIC      });
// MAGIC      d3.selectAll("text").attr("x", function (d) {
// MAGIC          return d.x;
// MAGIC      })
// MAGIC          .attr("y", function (d) {
// MAGIC          return d.y;
// MAGIC      });
// MAGIC  });
// MAGIC  </script>
// MAGIC  </html>
// MAGIC  """)
// MAGIC  }
// MAGIC    
// MAGIC    def help() = {
// MAGIC  displayHTML("""
// MAGIC  <p>
// MAGIC  Produces a force-directed graph given a collection of edges of the following form:</br>
// MAGIC  <tt><font color="#a71d5d">case class</font> <font color="#795da3">Edge</font>(<font color="#ed6a43">src</font>: <font color="#a71d5d">String</font>, <font color="#ed6a43">dest</font>: <font color="#a71d5d">String</font>, <font color="#ed6a43">count</font>: <font color="#a71d5d">Long</font>)</tt>
// MAGIC  </p>
// MAGIC  <p>Usage:<br/>
// MAGIC  <tt>%scala</tt></br>
// MAGIC  <tt><font color="#a71d5d">import</font> <font color="#ed6a43">d3._</font></tt><br/>
// MAGIC  <tt><font color="#795da3">graphs.force</font>(</br>
// MAGIC  &nbsp;&nbsp;<font color="#ed6a43">height</font> = <font color="#795da3">500</font>,<br/>
// MAGIC  &nbsp;&nbsp;<font color="#ed6a43">width</font> = <font color="#795da3">500</font>,<br/>
// MAGIC  &nbsp;&nbsp;<font color="#ed6a43">clicks</font>: <font color="#795da3">Dataset</font>[<font color="#795da3">Edge</font>])</tt>
// MAGIC  </p>""")
// MAGIC    }
// MAGIC  }

// COMMAND ----------

// MAGIC %scala
// MAGIC import d3._
// MAGIC  
// MAGIC  graphs.force(height = 800,width = 1000,
// MAGIC               clicks = sql("""SELECT 
// MAGIC                                    prev_title AS src,
// MAGIC                                    curr_title AS dest,
// MAGIC                                    n AS count FROM clicks_table
// MAGIC                              WHERE 
// MAGIC                                    curr_title IN ('Joaquin_Guzman_Loera', 'Enrique_Peña_Nieto') AND
// MAGIC                                    prev_id IS NOT NULL AND NOT (curr_title = 'Main_Page' OR prev_title = 'Main_Page')
// MAGIC                              ORDER BY n DESC
// MAGIC                              LIMIT 20""").as[Edge])

// COMMAND ----------

// MAGIC %scala
// MAGIC import d3._
// MAGIC  
// MAGIC  graphs.force(height = 800,width = 1000,
// MAGIC               clicks = sql("""SELECT 
// MAGIC                                    prev_title AS src,
// MAGIC                                    curr_title AS dest,
// MAGIC                                    n AS count FROM clicks_table
// MAGIC                              WHERE 
// MAGIC                                    curr_title IN ('Socio','Costco', 'Sams_Club') AND
// MAGIC                                    prev_id IS NOT NULL AND NOT (curr_title = 'Main_Page' OR prev_title = 'Main_Page')
// MAGIC                              ORDER BY n DESC
// MAGIC                              LIMIT 20""").as[Edge])

// COMMAND ----------

// MAGIC %scala
// MAGIC import d3._
// MAGIC  
// MAGIC  graphs.force(height = 800,width = 1000,
// MAGIC               clicks = sql("""SELECT 
// MAGIC                                    prev_title AS src,
// MAGIC                                    curr_title AS dest,
// MAGIC                                    n AS count FROM clicks_table
// MAGIC                              WHERE 
// MAGIC                                    curr_title IN ('Starbucks', 'Coffe') AND
// MAGIC                                    prev_id IS NOT NULL AND NOT (curr_title = 'Main_Page' OR prev_title = 'Main_Page')
// MAGIC                              ORDER BY n DESC
// MAGIC                              LIMIT 20""").as[Edge])

// COMMAND ----------


