```scala
package odkl.analysis.spark.feed.layout

import odkl.analysis.spark.test.AnalysisSparkFlatSpec
import org.apache.spark.ml.clustering.BisectingKMeansModel

import org.apache.spark.ml.PipelineModel
import org.mockito.Mockito.{mock, when}

case class CustomersFeedChannels
(
  age: Int,
  profile_age: Int,
  maxPositionBanner: Long,
  ...
)

class PrepareUserClustersJobSpec extends AnalysisSparkFlatSpece {
  import testImplicits._

  val settings = mock(classOf[PrepareUserClusterJobSettings])
  when(settings.nClusters).thenReturn(4)
  when(settings.randomSeed).thenReturn(42)

  "PrepareUserClusterJobSpec" should "fit pipeline with bisect-kmeans" in {
    val X = Seq(
      CustomersFeedChannels(48, 4277, 125, 13, 0, 1205, 7896, ...)
      CustomersFeedChannels(48, 4277, 125, 13, 0, 1205, 7896, ...)
      CustomersFeedChannels(48, 4277, 125, 13, 0, 1205, 7896, ...)
      ...
    ).toDF()

    val pipelineTrained: PipelineModel = PrepareUserClustersJob.trainBisectKMeans(X, settings)
    pipelineTrained.stages(0) shouldBe a [VectorAssembler]
    pipelineTrained.stages(1) shouldBe a [StandardScalerModel]
  }
}
```