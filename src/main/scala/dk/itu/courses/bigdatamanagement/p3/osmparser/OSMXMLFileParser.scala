package dk.itu.courses.bigdatamanagement.p3.osmparser


import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object OSMParser {

  org.apache.log4j.Logger getLogger "org" setLevel (org.apache.log4j.Level.OFF)
  org.apache.log4j.Logger getLogger "akka" setLevel (org.apache.log4j.Level.OFF)

  val spark = SparkSession.builder
    .master("local") //remove this when you need to deploy on cluster
    .getOrCreate
  
  spark.sparkContext.setLogLevel("OFF")
  import spark.implicits._


  //classes for the objects in the osm xml file

  // _crossingEdges, _from, _function, _id, _name, _priority, _shape, _spreadType, _to, _type, lane;
  case class Edge(
                   _id: String,
                   _function:String,
                   _from: String,
                   _to: String,
                   _type: String,
                   _priority:Int,
                   _shape: List[String],
                   _spreadType:String,
                   _name:String,
                   _crossingEdges:List[String]
                 )

//_allow, _disallow, _id, _index, _length, _shape, _speed, _width, param
  case class Lane(
                   _id: String,
                   _index: Int,
                   _allow: List[String],
                   _disallow: List[String],
                   _length: Float,
                   _speed: Float,
                   _width:Float,
                   _shape: List[String]
                 )

  //_id, _incLanes, _intLanes, _shape, _type, _x, _y, param, request;
  case class Junction(
                   _id: String,
                   _type:String,
                   _x: String,
                   _y: String,
                   _shape: List[String],
                   _incLanes: List[String],
                   _intLanes: List[String]
                 )





  //load an xml file
  def loadXMLFile(path:String, rowTag:String) = {
    spark
      .read
      .format("com.databricks.spark.xml")
      .option("rowTag",rowTag)
      .load(path)
  }


  //parse the xml file to extract edges
  def parsingOSMMapEdges(mapDF:DataFrame) ={

    val edgesDS: Dataset[Row] = mapDF
      .select(explode(col("edge")))
      .toDF("edge")
      .select(
        col("edge._id"),
        col("edge._function"),
        col("edge._from"),
        col("edge._to"),
        col("edge._type"),
        col("edge._priority"),
        col("edge._shape"),
        col("edge._spreadType"),
        col("edge._name"),
        col("edge._crossingEdges"))
      .as("Edge")

    edgesDS.
      repartition(1)
      .write
      .option("header", "true")
      .json("osmMapEdges.json")

  }


  //parse the xml file to extract lanes (of edges)
  def parsingOSMMapLanes(mapDF:DataFrame) ={

    val lanesDS: Dataset[Row] = mapDF
      .select(explode(col("edge")))
      .toDF("edge")
      .select(col("edge.lane"))
      .toDF("lane")
      .select(
        col("lane._id"),
        col("lane._index"),
        col("lane._allow"),
        col("lane._disallow"),
        col("lane._length"),
        col("lane._speed"),
        col("lane._width"),
        col("lane._shape"))
      .as("Lane")

    lanesDS.
      repartition(1)
      .write
      .option("header", "true")
      .json("osmMapLanes.json")

  }

  //parse the xml file to extract junctions
  def parsingOSMMapJunctions(mapDF:DataFrame) ={

    val junctionsDS: Dataset[Row] = mapDF
      .select(explode(col("junction")))
      .toDF("junction")
      .select(
        col("junction._id"),
        col("junction._type"),
        col("junction._x"),
        col("junction._y"),
        col("junction._incLanes"),
        col("junction._intLanes"),
        col("junction._shape"))
      .as("Junction")



    junctionsDS.
      repartition(1)
      .write
      .option("header", "true")
      .json("osmMapjunctions.json")

  }


  def main(args: Array[String]): Unit = {

    //change to where your file is located
    val loadedMapDF: DataFrame = loadXMLFile("./src/main/resources/osm.net.xml","net")

    //parse xml file of the map
    parsingOSMMapEdges(loadedMapDF)
    parsingOSMMapLanes(loadedMapDF)
    parsingOSMMapJunctions(loadedMapDF)
  
    spark.
      stop
  }

}

