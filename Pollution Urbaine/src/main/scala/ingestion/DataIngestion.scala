package ingestion
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object DataIngestion {
  def run(spark: SparkSession, path: String): DataFrame = {
    // -------------------------------
    // Read CSV file
    // -------------------------------
    println(s"=== Loading data from: $path")
    val df = spark.read
      .option("header", "true")        // first row as column names
      .option("inferSchema", "true")   // automatically detect column types
      .option("sep", ";")             // specify the semicolon separator
      .csv(path)

    // -----------------------------------------
    // Verify that the data loaded correctly
    // -----------------------------------------
    println("\n=== Preview first 5 rows ===")
    df.show(5)

    // -----------------------------------------
    // Type of Features
    // -----------------------------------------
    println("=== Schema of Data ===")
    df.printSchema()

    println(s"\nTotal rows: ${df.count()}")
    println(s"Total columns: ${df.columns.length}")

    // -----------------------------------------
    // Drop unusual columns
    // -----------------------------------------
    println("\n=== Dropping unusual columns...")
    val columnsToDrop = Seq("Country Code", "Coordinates", "Source Name", "Unit", "Country Label")
    val dropColumnsDF = df.drop(columnsToDrop: _*)

    // -----------------------------------------
    // Remove non-ASCII characters
    // -----------------------------------------
    println("\n=== Removing non-ASCII characters...")
    val df_fixedEncoding = dropColumnsDF
      .withColumn("City", regexp_replace(col("City"), "[^\\x00-\\x7F]", ""))
      .withColumn("Location", regexp_replace(col("Location"), "[^\\x00-\\x7F]", ""))

    // -----------------------------------------
    // Count Missing Values
    // -----------------------------------------
    // Step 1: Get string columns
    val stringCols = df_fixedEncoding.schema.fields.filter(_.dataType == StringType).map(_.name)

    // Step 2: Count null OR blank (for string columns)
    println("\n=== Missing values per column ===")
    df_fixedEncoding.select(stringCols.map { c =>sum( when(col(c).isNull || trim(col(c)) === "", 1)
      .otherwise(0) ).alias(c)}: _*)
      .show(false)

    // Step 3: Checking Duplicates values
    println("\n=== Checking Duplicates values...")
    val totalRows = df_fixedEncoding.count()
    val uniqueRows = df_fixedEncoding.dropDuplicates().count()
    println(s"Duplicates found: ${totalRows - uniqueRows}")


    // Step 4: Clean the data (remove duplicates & nulls)
    println("\n=== Cleaning the data (remove duplicates & nulls...")
    val df_noBlanks = stringCols.foldLeft(df_fixedEncoding) { (acc, colName) =>
      acc.withColumn(
        colName,
        when(trim(col(colName)) === "" || col(colName).isNull, null)
          .otherwise(col(colName))
      )
    }

    val cleaned_df = df_noBlanks.dropDuplicates().na.drop()

    println("\n=== Check Again Missing values per column...")
    cleaned_df.select(stringCols.map { c =>sum( when(col(c).isNull || trim(col(c)) === "", 1)
        .otherwise(0) ).alias(c)}: _*)
      .show(false)

    // -----------------------------------------
    // Removing outlier
    // -----------------------------------------
    println("\n=== Removing outlier...")
    val df_noOutliers = cleaned_df.filter(
      col("Value") >= 0 &&
        (
          (col("Pollutant") === "PM10" && col("Value") <= 500) ||
            (col("Pollutant") === "PM2.5" && col("Value") <= 300) ||
            (col("Pollutant") === "PM1" && col("Value") <= 100) ||
            (col("Pollutant") === "BC" && col("Value") <= 50) ||
            (col("Pollutant") === "NO" && col("Value") <= 200) ||
            (col("Pollutant") === "NO2" && col("Value") <= 400) ||
            (col("Pollutant") === "NOX" && col("Value") <= 500) ||
            (col("Pollutant") === "O3" && col("Value") <= 300) ||
            (col("Pollutant") === "CO" && col("Value") <= 50) ||
            (col("Pollutant") === "SO2" && col("Value") <= 100)
          )
    )
    println(s"\nRaw Data Size: ${df.count()} rows")
    println(s"Cleaned Data Size After Removing Missing Values: ${cleaned_df.count()} rows")
    println(s"Cleaned Data Size After Filtering Outlier Values:: ${df_noOutliers.count()} rows")


    println("Unique cities: " + df_noOutliers.select("City").distinct().count())
    println("Unique locations: " + df_noOutliers.select("Location").distinct().count())
    println("Unique pollutants: " + df_noOutliers.select("Pollutant").distinct().count())
    val uniquePollutants = df_noOutliers.select("Pollutant").distinct().collect().map(_.getString(0)).toList
    println("Unique pollutants values: " + uniquePollutants)
    println("\n=== Summary Statistics ===")
    df_noOutliers.describe().show()

    println("\n=== Final Dataset after Completing Ingestion ===")
    df_noOutliers.show(10)
    df_noOutliers
  }
}
