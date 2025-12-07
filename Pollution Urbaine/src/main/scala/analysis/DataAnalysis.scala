package analysis
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataAnalysis {
  def run(spark: SparkSession, df: DataFrame): Unit = {


    // --------------------------------------------------------------
    // Calculate average pollution level per station (City + Location)
    // --------------------------------------------------------------
    println("\n=== Descriptive Analysis ===")
    // Step 1: Stations Most Exposed to Pollution
    println("Stations Most Exposed to Pollution")
    val stationExposure = df.groupBy("City", "Location")
      .agg(
        avg("Value").alias("avg_pollution"),
        max("Value").alias("peak_pollution"),
        count("*").alias("num_measurements")
      )
      .orderBy(desc("avg_pollution"))

    stationExposure.show(false)


    // Step 2: Group by hour of day to see when pollution peaks
    println("Detection of Hourly Peaks & Critical Periods")
    val hourlyPeaks = df.groupBy("hour", "Pollutant")
      .agg(avg("Value").alias("avg_value"))
      .orderBy(desc("avg_value"))

    hourlyPeaks.show(false)

    // Step 3: Average, min, max per pollutant
    println("Pollutant Trends - Average, Min, Max per pollutant")
    val pollutantStats = df.groupBy("Pollutant")
      .agg(
        avg("Value").alias("avg_value"),
        min("Value").alias("min_value"),
        max("Value").alias("max_value"),
        count("*").alias("num_measurements")
      )
      .orderBy(desc("avg_value"))

    pollutantStats.show(false)

    // Step 4: Correlation between pollutants (pivot table style)
    println("\n=== Correlations / Relationships ===")
    val pivotDF = df.groupBy("City", "Location", "Last_Updated")
      .pivot("Pollutant")
      .agg(avg("Value"))

    val pollutantCols = pivotDF.columns.filter(_ != "City").filter(_ != "Location").filter(_ != "Last_Updated")
    for (i <- pollutantCols.indices) {
      for (j <- i + 1 until pollutantCols.length) {
        val corrVal = pivotDF.stat.corr(pollutantCols(i), pollutantCols(j))
        println(s"Correlation between ${pollutantCols(i)} and ${pollutantCols(j)}: $corrVal")
      }
    }

    // --------------------------------------------------------------
    // Aggregated Metrics
    // --------------------------------------------------------------
    println("\n=== Aggregated Metrics ===")
    // Step 1: Average pollution per season
    println("Average Pollution per Season")
    val seasonStats = df.groupBy("season", "Pollutant")
      .agg(avg("Value").alias("avg_value"), max("Value").alias("max_value"))
      .orderBy("season", "Pollutant")

    seasonStats.show(false)

    // Step 2: Average pollution per month
    println("Average Pollution per Month")
    val monthStats = df.groupBy("Month", "Pollutant")
      .agg(avg("Value").alias("avg_value"))
      .orderBy("Month")

    monthStats.show(false)

    // Step 3: Weekday vs Weekend
    println("Average Pollution by Weekday vs Weekend")
    val weekendStats = df.groupBy("is_weekend", "Pollutant")
      .agg(avg("Value").alias("avg_value"))
      .orderBy(desc("is_weekend"))

    weekendStats.show(false)

    // Step 4: Count of high pollution events per station (threshold example: Value > 100)
    println("Count of High Pollution Events Per Station")
    val highPollutionCount = df.filter(col("Value") > 100)
      .groupBy("City", "Location", "Pollutant")
      .agg(count("*").alias("high_pollution_count"))
      .orderBy(desc("high_pollution_count"))

    highPollutionCount.show(false)

    // Step 5: Rolling statistics already exist, but you can summarize per station
    println("Rolling statistics already exist, but you can summarize per station")
    val stationRollingStats = df.groupBy("City", "Location", "Pollutant")
      .agg(
        avg("rolling_mean_3h").alias("avg_rolling_mean"),
        max("rolling_max_3h").alias("max_rolling"),
        min("rolling_min_3h").alias("min_rolling")
      )
      .orderBy(desc("avg_rolling_mean"))

    stationRollingStats.show(false)

    // --------------------------------------------------------------
    // Automatic Anomaly Detection
    // --------------------------------------------------------------
    println("\n=== Automatic Anomaly Detection ===")
    val meanStd = df.agg(mean("Value"), stddev("Value")).first()
    val meanVal = meanStd.getDouble(0)
    val stdVal = meanStd.getDouble(1)

    val anomalies = df.filter(
      (col("Value") > meanVal + 3 * stdVal) || (col("Value") < meanVal - 3 * stdVal)
    )
    println(s"Number of anomalies detected: ${anomalies.count()}")
    anomalies.show(false)
  }
}