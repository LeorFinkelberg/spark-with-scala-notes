Подробности можно найти здесь https://spark.apache.org/docs/latest/ml-clustering.html#bisecting-k-means

NB! `VectorAssembler` в выходной кадр данных включает все атрибуты, которые были переданы этому классу, а в `features` включает только атрибуты, указанные в `.setInputCols()`. Таким образом, например, можно передать кадр данных, который включает атрибут `userId`, а в `.setInputCols()` указать все атрибуты кроме `userId`, чтобы модель на нем обучалась. Тогда при построении прогноза итоговый кадр данных будет включать и `userId` и `predictions`.

```scala
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{Normalizer, RobustScaler}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.{BisectingKMeans, BisectingKMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator

val RandomSeed = 42
val Nclusters = 4
val FeaturesColName = "features"
val ScaledFeaturesColName = "scaledFeatures"
val PredictionColName = "prediction"

// Предполагается, что есть подготовленный набор данных X, содержащий только вещественные признаки
val assembler = new VectorAssembler()
  .setInputCols(X.columns)
  .setOutputCol(FeaturesColName)

val scaler = new RobustScaler()
  .setInputCol(FeaturesColName)
  .setOutputCol(ScaledFeaturesColName)

val bisectKmeans = new BisectingKMeans()
  .setK(Nclusters)
  .setSeed(RandomSeed)
  .setFeaturesCol(ScaledFeaturesColName)
  .setPredictionCol(PredictionColName)

val pipeline = new Pipeline()
  .setStages(
    Array(
      assembler,
      scaler,
      bisectKmeans
    )
  )

val model = pipeline.fit(X)

// Обученную модель можно сохранить
model.write.overwrite().save("user/alexander.podvoyskiy/models/bisect-kmeans-model")

// А затем снова прочитать
val bisectKmeansModel =
  PipelineModel.load("user/alexander.podvoyskiy/models/bisect-kmeans-model")

val predictions = bisectKmeansModel.transform(X)

// Распределение меток
predictions.groupBy("prediction").count().show()

// Центройды
val centers = bisectKmeansModel
  .stages(2).asInstanceOf[BisectingKMeansModel]
  .clusterCenters

centers.foreach(println)

// Оценим качество
val evaluatorSilhouette = new ClusteringEvaluator()
val silhouette = evaluatorSilhouette.evaluate(predictions)
```
