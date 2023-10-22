import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import com.scaliot.data._
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Scaliot")
      .config("spark.master", "local")
      .getOrCreate()

    spark
      .sparkContext
      .hadoopConfiguration
      .setClass("fs.file.impl", classOf[BareLocalFileSystem], classOf[FileSystem])

    val dataLoader = new DataLoader(spark)
    val eventsData = dataLoader.loadCSV(getClass.getResource(EventsPath).getPath)
    val locationsData = dataLoader.loadCSV(getClass.getResource(LocationsPath).getPath)
    val productsData = dataLoader.loadCSV(getClass.getResource(ProductsPath).getPath)
    val sensorTypes = dataLoader.loadCSV(getClass.getResource(SensorTypesPath).getPath)

    // Get most transacted products
    val mostMovedProducts = eventsData
      .filter(col("event_type") === "START")
      .groupBy(col("product_id"))
      .count
      .sort(col("count").desc)
      .limit(5)

    mostMovedProducts.show

    val mostMovedProductsNames = mostMovedProducts
      .join(productsData, Seq("product_id"), "inner")
      .select(col("product_description"))
      .collect
      .toList
      .map(row => row.getString(0))
      .mkString(", ")


    println(s"The 5 most moved products were ${mostMovedProductsNames}")

    val lostProducts = eventsData
      .filter("event_type in ('START', 'END')")
      .groupBy(col("process_id"), col("product_id"))
      .count()
      .filter(col("count") === 1)
      .count

    println(s"A total of ${lostProducts} products have been lost")
  }
}