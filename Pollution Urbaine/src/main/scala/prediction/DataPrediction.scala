package prediction

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.regression.{RandomForestRegressor, DecisionTreeRegressor, GBTRegressor, LinearRegression}
import org.apache.spark.ml.evaluation.RegressionEvaluator

object DataPrediction {
  def run(spark: SparkSession, df: DataFrame): Unit = {
    import spark.implicits._

    // ------------------------------------
    // Mean Encoding for City & Location
    // ------------------------------------
    println("=== Encoding categorical features ===")
    println("Step 1: City and Location mean encoding...")
    val dfWithMeans = df
      .join(df.groupBy("City").agg(mean("global_index").alias("City_encode")), Seq("City"), "left")
      .join(df.groupBy("Location").agg(mean("global_index").alias("Location_encode")), Seq("Location"), "left")

    // Step 2: Pollutant OneHotEncoder encoding
    println("Step 2: Pollutant OneHotEncoder encoding...")
    val pollutantIndexer = new StringIndexer()
      .setInputCol("Pollutant")
      .setOutputCol("Pollutant_index_temp")
      .setHandleInvalid("keep")
    val dfIndexedPollutant = pollutantIndexer.fit(dfWithMeans).transform(dfWithMeans)

    val pollutantEncoder = new OneHotEncoder()
      .setInputCol("Pollutant_index_temp")
      .setOutputCol("Pollutant_encode")

    val dfEncodedPollutant = pollutantEncoder.fit(dfIndexedPollutant)
      .transform(dfIndexedPollutant)
      .drop("Pollutant_index_temp")

    // Step 3: Season OneHotEncoder encoding
    println("Step 3: Season OneHotEncoder encoding...")
    val seasonIndexer = new StringIndexer()
      .setInputCol("season")
      .setOutputCol("Season_index_temp")
      .setHandleInvalid("keep")

    val dfIndexedSeason = seasonIndexer.fit(dfEncodedPollutant).transform(dfEncodedPollutant)

    val seasonEncoder = new OneHotEncoder()
      .setInputCol("Season_index_temp")
      .setOutputCol("Session_encode")

    val dfEncoded = seasonEncoder.fit(dfIndexedSeason)
      .transform(dfIndexedSeason)
      .drop("Season_index_temp")

    println("Columns after encoding: " + dfEncoded.columns.mkString(", "))
    dfEncoded.show(false)

    // -------------------------------
    // Features Selection
    // -------------------------------
    // Candidate features (exclude target & raw categorical/time columns)
    println("=== Features Selection ===")
    println("Step 1: Candidate features...")
    val candidateFeatures = dfEncoded.columns
      .filterNot(c => Set(
        "global_index", "Pollutant", "City", "Location", "season", "Last_Updated", "ts"
      ).contains(c))

    println("Candidate features:")
    candidateFeatures.foreach(println)


    // Step 2: Assemble all candidate features into 'features' vector
    println("Step 2: Assemble all candidate features into 'features' vector...")
    val assembler = new VectorAssembler()
      .setInputCols(candidateFeatures)
      .setOutputCol("features")

    val dfWithFeatures = assembler.transform(dfEncoded)
    dfWithFeatures.select("features", "global_index").show(5, false)

    // Step 3: Split dataset into train/test
    println("Step 3: Split dataset into train/test...")
    val Array(trainDF_all, testDF_all) = dfWithFeatures.randomSplit(Array(0.8, 0.2), seed = 42)
    println(s"Train count: ${trainDF_all.count()}, Test count: ${testDF_all.count()}")


    // Step 4: Train Random Forest to get feature importances
    println("Step 4: Train Random Forest to get feature importances...")
    val rfForImportance = new RandomForestRegressor()
      .setLabelCol("global_index")
      .setFeaturesCol("features")
      .setNumTrees(50)
      .setSeed(42)

    val rfImportanceModel = rfForImportance.fit(trainDF_all)

    // Step 5: Select top N features based on importance
    println("Step 5: Select top N features based on importance...")
    val importances = rfImportanceModel.featureImportances
    println("Feature importances (feature -> importance):")
    candidateFeatures.zip(importances.toArray).foreach { case (f, imp) =>
      println(f"$f: $imp%.6f")
    }

    val topN = 10
    val topFeatures = candidateFeatures.zip(importances.toArray)
      .sortBy(-_._2)
      .take(topN)
      .map(_._1)

    println(s"Top $topN features as list: " + topFeatures.toList)

    // ---------------------------------------------
    // Assemble top features only -> features_top
    // ---------------------------------------------
    val assemblerTop = new VectorAssembler()
      .setInputCols(topFeatures)
      .setOutputCol("features_top")

    val trainTop = assemblerTop.transform(trainDF_all).cache()
    val testTop = assemblerTop.transform(testDF_all).cache()

    trainTop.select("features_top", "global_index").show(5)

    // -------------------------------
    // Prepare evaluator
    // -------------------------------
    // Step 1: Prepare Root Mean Squared Error
    val evaluatorRMSE = new RegressionEvaluator()
      .setLabelCol("global_index")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    // Step 2: R² or the coefficient of determination
    val evaluatorR2 = new RegressionEvaluator()
      .setLabelCol("global_index")
      .setPredictionCol("prediction")
      .setMetricName("r2")

    // -------------------------------
    // Train & Evaluate multiple models
    // -------------------------------
    // Step 1: Variable Declaration
    println("\nVariable Declaration...")
    case class ModelResult(name: String, rmse: Double, r2: Double)

    var results = Seq.empty[ModelResult]

    // Step 2: Random Forest
    println("\nRandom Forest...")
    val rf = new RandomForestRegressor()
      .setLabelCol("global_index")
      .setFeaturesCol("features_top")
      .setNumTrees(50)
      .setSeed(42)

    val rfModel = rf.fit(trainTop)
    val rfPred = rfModel.transform(testTop)
    val rfRmse = evaluatorRMSE.evaluate(rfPred)
    val rfR2 = evaluatorR2.evaluate(rfPred)
    println(s"[RandomForest] RMSE=$rfRmse, R2=$rfR2")
    results :+= ModelResult("RandomForest", rfRmse, rfR2)

    // Step 3: Decision Tree
    println("\nDecision Tree...")
    val dt = new DecisionTreeRegressor()
      .setLabelCol("global_index")
      .setFeaturesCol("features_top")
      .setSeed(42)

    val dtModel = dt.fit(trainTop)
    val dtPred = dtModel.transform(testTop)
    val dtRmse = evaluatorRMSE.evaluate(dtPred)
    val dtR2 = evaluatorR2.evaluate(dtPred)
    println(s"[DecisionTree] RMSE=$dtRmse, R2=$dtR2")
    results :+= ModelResult("DecisionTree", dtRmse, dtR2)

    // Step 3: Gradient-Boosted Trees
    println("\nGradient-Boosted Trees...")
    val gbt = new GBTRegressor()
      .setLabelCol("global_index")
      .setFeaturesCol("features_top")
      .setMaxIter(50)
      .setSeed(42)

    val gbtModel = gbt.fit(trainTop)
    val gbtPred = gbtModel.transform(testTop)
    val gbtRmse = evaluatorRMSE.evaluate(gbtPred)
    val gbtR2 = evaluatorR2.evaluate(gbtPred)
    println(s"[GBT] RMSE=$gbtRmse, R2=$gbtR2")
    results :+= ModelResult("GBT", gbtRmse, gbtR2)

    // Step 4: Linear Regression (baseline)
    println("\nLinear Regression...")
    val lr = new LinearRegression()
      .setLabelCol("global_index")
      .setFeaturesCol("features_top")
      .setMaxIter(100)
      .setRegParam(0.0)

    val lrModel = lr.fit(trainTop)
    val lrPred = lrModel.transform(testTop)
    val lrRmse = evaluatorRMSE.evaluate(lrPred)
    val lrR2 = evaluatorR2.evaluate(lrPred)
    println(s"[LinearRegression] RMSE=$lrRmse, R2=$lrR2")
    results :+= ModelResult("LinearRegression", lrRmse, lrR2)

    // -----------------------------------------------------
    // Select best model by RMSE (lower is better)
    // -----------------------------------------------------
    val best = results.minBy(_.rmse)
    println(s"=== Best model: ${best.name} with RMSE=${best.rmse} and R2=${best.r2} ===")

    // Unpersist cached datasets
    trainTop.unpersist()
    testTop.unpersist()
  }
}