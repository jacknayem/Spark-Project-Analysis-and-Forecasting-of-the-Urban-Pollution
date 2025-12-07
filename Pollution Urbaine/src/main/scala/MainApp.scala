import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import ingestion.DataIngestion
import transformation.DataTransformation
import analysis.DataAnalysis
import modeling.DataModeling
import prediction.DataPrediction

object MainApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Pollution Urbaine")
      .master("local[*]")
      .config("spark.driver.extraJavaOptions", "--add-opens java.base/java.util=ALL-UNNAMED")
      .config("spark.executor.extraJavaOptions", "--add-opens java.base/java.util=ALL-UNNAMED")
      .getOrCreate()

    // Set Hadoop home directory for Windows (fixes win_utils.exe warning)
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    // Reduce Spark logging
    Logger.getLogger("org").setLevel(Level.WARN)  // only show WARN or ERROR
    Logger.getLogger("akka").setLevel(Level.WARN)

    // -------------------------------
    // Step 1: Ingestion
    // -------------------------------
    val Clean_Data = DataIngestion.run(spark, "src/main/resources/openaq.csv")

    // -------------------------------
    // Step 2: Transformation
    // -------------------------------
    val transformedData = DataTransformation.run(Clean_Data, spark)

    // -------------------------------
    // Step 3: In-depth analysis
    // -------------------------------
    DataAnalysis.run(spark, transformedData)

    // -------------------------------
    // Step 4: Modeling
    // -------------------------------
    DataModeling.run(spark, transformedData)

    // -------------------------------
    // Step 5: Prediction
    // -------------------------------
    DataPrediction.run(spark, transformedData)

    spark.stop()
  }
}