###  Как переиспользовать объекты в тест-блоках

Детали можно найти здесь https://www.scalatest.org/user_guide/sharing_fixtures

Здесь предполагается, что основная Spark-джоба лежит по пути 
```bash
odnoklassnkiki-ml-core/
  odnoklassniki-analsys/
    scala/
      odkl/
        analysis/
          spark/
            feed/
              layout/
                PrepareUserClustersJob.scala
```
а тесты лежат здесь
```bash
odnoklassnkiki-ml-core/
  odnoklassniki-analsys/
    scalatest/
      odkl/
        analysis/
          spark/
            feed/
              layout/
                PrepareUserClustersJobSpec.scala
```
```scala
// PrepareUserClustersJobSpec.scala
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

  private def fixture: Object {
    val X: DataFrame,
    val pipelineTrained: Pipeline
  } = 
    new {
      val X: DataFrame = Seq(
	      CustomersFeedChannels(48, 4277, 125, 13, 0, 1205, 7896, ...)
	      CustomersFeedChannels(48, 4277, 125, 13, 0, 1205, 7896, ...)
	      CustomersFeedChannels(48, 4277, 125, 13, 0, 1205, 7896, ...)
	      ...
	    ).toDF()

      // Предполагается, что метод `trainBisectKMeans`
      // использует settings.nClusters etc., поэтому приходится
      // явно передавать `settings`
      val pipelineTrained: PipelineModel =
        PrepareUserClustersJob.trainBisectKMeans(X, settings)
    }

  // Первый тест
  "PrepareUserClusterJobSpec" should "fit pipeline with bisect-kmeans" in {
    val f = fixture

    f.pipelineTrained.stages(0) shouldBe a [VectorAssembler]
    pipelineTrained.stages(1) shouldBe a [StandardScalerModel]
  }

  // Второй тест
  it should "catch AssertationErroron ..." in {
    intercept[AssertationError] {
      PrepareUserClustersJob.getSilhouette(f.pipelineTrained.transform(f.X))
    }
  }
}
```

В принципе, если не нравится ставить `f.` перед каждым именем переменной, то можно сделать так
```scala
...
it should "..." in {
  val f = fixture
  import f._
}
```

Еще можно элементы, которые мы собираемся переиспользовать в различных тестовых блоках обернуть трейтами
```scala
package org.scalatest.examples.flatspec.fixturecontext
  
import collection.mutable.ListBuffer
import org.scalatest.flatspec.AnyFlatSpec
  
class ExampleSpec extends AnyFlatSpec {
  
  trait Builder {
    val builder = new StringBuilder("ScalaTest is ")
  }
  
  trait Buffer {
    val buffer = ListBuffer("ScalaTest", "is")
  }
  
  // This test needs the StringBuilder fixture
  "Testing" should "be productive" in new Builder {
    builder.append("productive!")
    assert(builder.toString === "ScalaTest is productive!")
  }
  
  // This test needs the ListBuffer[String] fixture
  "Test code" should "be readable" in new Buffer {
    buffer += ("readable!")
    assert(buffer === List("ScalaTest", "is", "readable!"))
  }
  
  // This test needs both the StringBuilder and ListBuffer
  it should "be clear and concise" in new Builder with Buffer {
    builder.append("clear!")
    buffer += ("concise!")
    assert(builder.toString === "ScalaTest is clear!")
    assert(buffer === List("ScalaTest", "is", "concise!"))
  }
}
```