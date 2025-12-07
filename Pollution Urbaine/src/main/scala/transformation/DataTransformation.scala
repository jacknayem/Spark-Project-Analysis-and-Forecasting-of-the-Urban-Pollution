package transformation
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.expressions.Window

object DataTransformation {
  def run(df: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    // -------------------------------
    // Normalize text
    // -------------------------------
    println("\n=== Normalizing City, Location, Pollutant...")
    val normalizedDF = df
      .withColumn("City", lower(col("City")))             // Convert to lowercase
      .withColumn("Location", lower(col("Location")))     // Convert to lowercase
      .withColumn("Pollutant", upper(col("Pollutant")))   // Convert to uppercase

      // Replace unwanted characters with space
      .withColumn("City", regexp_replace(col("City"), "[^a-z0-9]", " "))
      .withColumn("Location", regexp_replace(col("Location"), "[^a-z0-9]", " "))
      .withColumn("Pollutant", regexp_replace(col("Pollutant"), "[^A-Z0-9]", "_"))

      // Convert multiple spaces into a single space
      .withColumn("City", regexp_replace(col("City"), "\\s+", " "))
      .withColumn("Location", regexp_replace(col("Location"), "\\s+", " "))
      .withColumn("Pollutant", regexp_replace(col("Pollutant"), "\\s+", " "))

      // Trim
      .withColumn("City", trim(col("City")))
      .withColumn("Location", trim(col("Location")))
      .withColumn("Pollutant", trim(col("Pollutant")))

      // Replace space characters with underscore
      .withColumn("City", regexp_replace(col("City"), " ", "_"))
      .withColumn("Location", regexp_replace(col("Location"), " ", "_"))

    // -------------------------------------------------
    // Clean the data (remove duplicates & nulls)
    // -------------------------------------------------
    println("\n=== Cleaning Missing values per column...")
    // Step 1: Get string columns
    val stringCols = normalizedDF.schema.fields.filter(_.dataType == StringType).map(_.name)

    // Step 4: Detect Blank and Null value
    val df_noBlanks = stringCols.foldLeft(normalizedDF) { (acc, colName) =>
      acc.withColumn(
        colName,
        when(trim(col(colName)) === "" || col(colName).isNull, null)
          .otherwise(col(colName))
      )
    }
    // Step 3: Drop Rows
    val MissVal_cleaned_df = df_noBlanks.dropDuplicates().na.drop()

    println(s"\nMain Data Size: ${df.count()} rows")
    println(s"Data Size After Removing Missing Values: ${MissVal_cleaned_df.count()} rows")

    println("\n=== Check Again Missing values per column ===")
    MissVal_cleaned_df.select(stringCols.map { c =>sum( when(col(c).isNull || trim(col(c)) === "", 1)
        .otherwise(0) ).alias(c)}: _*)
      .show(false)


    // -------------------------------------------------
    // Creating Hour, Day, Month and Year features
    // -------------------------------------------------
    println("\n=== Creating Hour, Day, Month and Year features...")
    val transformedDF = MissVal_cleaned_df
      .withColumn("Hour", hour(col("Last Updated")))
      .withColumn("Day", dayofmonth(col("Last Updated")))
      .withColumn("Month", month(col("Last Updated")))
      .withColumn("Year", year(col("Last Updated")))

    // -------------------------------------------------
    // Ensure types & rename to consistent columns
    // -------------------------------------------------
    println("\n=== Ensuring types & rename to consistent columns...")
    val mappedDF = transformedDF.map(row => {
      val city = row.getAs[String]("City")
      val location = row.getAs[String]("Location")
      val pollutant = row.getAs[String]("Pollutant")
      val value = row.getAs[Double]("Value")
      val lastUpdated = row.getAs[java.sql.Timestamp]("Last Updated")
      val hour = row.getAs[Int]("Hour")
      val day = row.getAs[Int]("Day")
      val month = row.getAs[Int]("Month")
      val year = row.getAs[Int]("Year")
      (city, location, pollutant, value, lastUpdated, hour, day, month, year)
    }).toDF("City", "Location", "Pollutant", "Value", "Last_Updated", "Hour", "Day", "Month", "Year")
      .filter(!$"Value".isNaN)

    // -------------------------------------------------
    // Count How Many Times Each Pollutant Appears
    // -------------------------------------------------
    println("\n=== Counting Pollutant Values...")
    val countsDF = mappedDF.groupBy("Pollutant").count().orderBy(desc("count"))
    countsDF.show()

    // -------------------------------------------------
    // Keeping Only Cities With count > 2
    // -------------------------------------------------
    println("\nKeeping only cities with count > 1...")
    // Step 1: Select
    val cityCounts = mappedDF.groupBy("City").count().filter(col("count") > 2)

    // Step 2: Join back to keep only rows with those cities
    val dfFiltered = mappedDF.join(cityCounts.select("City"), Seq("City"), "inner")


    // Check All The Data Group by City
    val citySummary = dfFiltered
      .groupBy("City")
      .agg(
        count("*").alias("NumRows"),
        countDistinct("Location").alias("NumLocations")
      )
      .orderBy(desc("NumRows"))
    citySummary.show(df.count().toInt)

    // -------------------------------------------------------------------------------------------------
    // flatMap example (split Pollutant if multiple pollutants in one column, e.g., "CO2,NO2")
    // -------------------------------------------------------------------------------------------------
    println("\n=== FlatMap example (split Pollutant if multiple pollutants in one column, e.g., CO2,NO2)...")
    val flatMappedDF = dfFiltered.flatMap(row => {
      val city = row.getAs[String]("City")
      val location = row.getAs[String]("Location")
      val pollutants = row.getAs[String]("Pollutant").split(",")
      val value = row.getAs[Double]("Value")
      val lastUpdated = row.getAs[java.sql.Timestamp]("Last_Updated")
      val hour = row.getAs[Int]("Hour")
      val day = row.getAs[Int]("Day")
      val month = row.getAs[Int]("Month")
      val year = row.getAs[Int]("Year")
      pollutants.map(p => (city, location, p.trim, value, lastUpdated, hour, day, month, year))
    }).toDF("City", "Location", "Pollutant", "Value", "Last_Updated", "Hour", "Day", "Month", "Year")

    println(s"\nTotal flatMappedDF rows: ${flatMappedDF.count()}")

    // -------------------------------------------------------------------------------------------------
    //  Create Features - avg_value, max_value, min_value, num_readings
    // -------------------------------------------------------------------------------------------------
    println("\n=== Create Features - avg_value, max_value, min_value, num_readings...")
    // Step 1: Calculate statistics per station (average, max, min) per pollutant
    val statsDF = flatMappedDF.groupBy("City", "Location", "Pollutant")
      .agg(
        avg("Value").alias("avg_value"),
        max("Value").alias("max_value"),
        min("Value").alias("min_value"),
        count("Value").alias("num_readings")
      )
    println(s"Total statsDF rows: ${statsDF.count()}")
    // Step 2: Add as a feature
    val dfWithStats = flatMappedDF.join(statsDF, Seq("City", "Location", "Pollutant"), "left")
    println(s"Total dfWithStats rows: ${dfWithStats.count()}")
    println(s"Total columns: ${dfWithStats.columns.length}")

    // -------------------------------------------------------------------------------------------------
    //  Create Features more
    // -------------------------------------------------------------------------------------------------
    //  Step 1: Creating Global Index Feature
    println("\n=== Creating Global Index Feature...")
    val windowSpec = Window.partitionBy("Pollutant")
    val dfWithNormalized = dfWithStats.withColumn("global_index",
      (col("Value") - min("Value").over(windowSpec)) /
        (max("Value").over(windowSpec) - min("Value").over(windowSpec))
    )
    dfWithNormalized.select("City", "Location", "Pollutant", "Value", "global_index").show(5)

    //  Step 2: Creating is_weekend Feature
    println("\n=== Creating is_weekend Feature...")
    val dfWithWeekend = dfWithNormalized.withColumn("is_weekend", when(dayofweek(col("Last_Updated")).isin(1,7), 1).otherwise(0))

    //  Step 3: Creating season Feature
    println("\n=== Creating season Feature...")
    val dfWithSeason = dfWithWeekend.withColumn("season",
      when(col("Month").between(3,5), "Spring")
        .when(col("Month").between(6,8), "Summer")
        .when(col("Month").between(9,11), "Autumn")
        .otherwise("Winter")
    )
    dfWithSeason.select("Day", "Month", "Year", "is_weekend", "season").show(5)

    //  Step 4: Creating ts and is_rush_hour Features
    println("\n=== Creating ts and is_rush_hour Features...")
    val dfWithTs = dfWithSeason.withColumn("ts", unix_timestamp(col("Last_Updated")).cast("long"))
    val dfWithRush = dfWithTs.withColumn("is_rush_hour",
      when(col("Hour").between(7, 9) || col("Hour").between(17, 19), 1).otherwise(0))
    dfWithRush.select("Last_Updated","ts","Hour", "is_rush_hour").show(5)

    //  Step 5: Creating rolling_mean_3h, rolling_max_3h and rolling_min_3h Features
    println("\n=== Creating rolling_mean_3h, rolling_max_3h and rolling_min_3h Features...")
    val rollingWindow = Window.partitionBy("City", "Location", "Pollutant")
      .orderBy("ts").rangeBetween(-10800, 0) // last 3 hours

    val dfWithRolling = dfWithRush
      .withColumn("rolling_mean_3h", avg(col("Value")).over(rollingWindow))
      .withColumn("rolling_max_3h", max(col("Value")).over(rollingWindow))
      .withColumn("rolling_min_3h", min(col("Value")).over(rollingWindow))
    dfWithRolling.select("Value", "rolling_mean_3h", "rolling_max_3h", "rolling_min_3h").show(5)

    //  Step 6: Creating value_z_score_by_pollutant Feature
    println("\n=== Creating value_z_score_by_pollutant Feature...")
    val pollutantWindow = Window.partitionBy("Pollutant")
    val dfWithZ = dfWithRolling
      .withColumn("pollutant_mean", avg(col("Value")).over(pollutantWindow))
      .withColumn("pollutant_std", stddev(col("Value")).over(pollutantWindow))
      .withColumn("value_z_score_by_pollutant",
        when(col("pollutant_std") === 0.0 || col("pollutant_std").isNull, 0.0)
          .otherwise((col("Value") - col("pollutant_mean")) / col("pollutant_std"))
      )
      .drop("pollutant_mean", "pollutant_std")
    dfWithZ.select("Value", "value_z_score_by_pollutant").show(5)

    //  Step 7: Creating station_pollutant_count Feature
    println("\n=== Creating station_pollutant_count Feature...")
    val stationCountWindow = Window.partitionBy("City", "Location", "Pollutant")
    val finalDF = dfWithZ.withColumn("station_pollutant_count", count(lit(1)).over(stationCountWindow))
    finalDF.select("station_pollutant_count").show(5)

    println(s"Total columns: ${finalDF.columns.length}")
    println("Columns: " + finalDF.columns.mkString(", "))

    println("\n=== Data Type ===")
    finalDF.printSchema()

    println("\n=== Check Again Missing values per column ===")
    val missingDF = finalDF.select(finalDF.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)):_*)
    missingDF.show()

    println("\n=== Final Dataset After Completing Data Transformation ===")
    finalDF.show(5)

    finalDF  // return the new DataFrame
  }
}

