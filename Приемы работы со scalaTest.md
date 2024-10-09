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

case class Customer
(
  user_id: Long,
  city_id: Long,
  birth_country_id: Option[Long], // передавать сюда можно либо Some(34345), либо null
  ...
)

// Эти структуры/кейс-классы нужны для описания конструкций типа
// array of struct в df.printSchema
case class ShowStruct(timestamp: Long, showMillis: Long)
case class ClicksStruct(timestamp: Long)

case class FeedChannels
(
  userId: Long,
  platform: Long,
  channel: String,
  ...
  targets: Map[String, Map[String, Double]],
  plannerExperiment: String,
  shows: Array[ShowStruct],
  clicks: Array[ClicksStruct],
)

class PrepareUserClustersJobSpec extends AnalysisSparkFlatSpece {
  import testImplicits._

  // Настройки PrepareUserClusterJobSettings описаны 
  // в файле PrepareUserClusterJob.scala
  val settings = mock(classOf[PrepareUserClusterJobSettings])
  when(settings.nClusters).thenReturn(4)
  when(settings.randomSeed).thenReturn(42)
  when(settings.meaningfulFeatureNames).thenReturn(
    Array(
      "age",
      "profile_age",
      ...
    )
  )

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

Здесь можно было бы разместить все наборы данных и прочие сущности в одном трейте `Fixture`
```scala
class PrepareUserClustersJobSpec extends AnalysisSparkFlatSpec {
    import testImplicits._

    ...
	trait Fixture {
	  val customers: DataFrame = Seq(  // последовательность case-class
	    Customer(
	      user_id = 4202088L,
	      city_id = ...,
	      birth_country_id = Some(103345345L),
	      ...
	    ),
	    Customer(
	      user_id = ...,
	      city_id = ...,
	      ...
	    )
	  ).toDF()
	
	  val feedChannels: DataFrame = Seq( // последовательность case-class
	    FeedChannel(
	      userId = 42020088L,
	      platform = 5,
	      ...
	      shows = Array(ShowStruct(1724934345L, 22489)) ,
	      clicks = Array(ClicksStruct(34534534534L)),
	    ),
	    ...
	  )
	}

    "PrepareUserClustersJobSpec" should "..." in new Fixture {
      ...
    }

    it should "get nCols of `feedChannels` dataset" in new Fixture {
      feedChannels.columns.length shouldBe 15
    }

    it should "get nCols of `customersPrepared` dataset" in new Fixture {
      customersPrepared.select("profile_age").collect()
        should contain theSameElements Array(Row(6273), Row(4753), Row(2394))
    }

    it should "..." in new Fixture {
    // `pipelineTrained` и `X` определены в Fixture

      val msgExpected = "assertion failed: Number of clusters ..."
        intercept[AssertationError] {
          PrepareUserClustersJob.getSilhouette(pipelineTrained.transform(X))
        }.getMessage should msgExpected
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

Проверить строку на соответствие регулярному выражению можно так
```scala
it should "completely match the regular expression pattern" in new Fixture {
  userSegmentIndexToProbTest.split(";")(0).split(":", 2)(1) should fullyMatch regex Constants.pattern.r
}
```

В этом примере предполагается, что в сценарии `Constants.scala` определен объект `Constants` , а в нем определена строка `pattern`, которую мы по месту превращаем в регулярное выражение с помощью метода `.r`.

Проверку на то, что множество содержит какой-то элемент можно организовать так
```scala
it should "check ..." in new Fixture {
  // круглые скобочки важны!
  value.getExperiments should contain ("default")
}
```

Проверить `TByteFloatHashMap` https://trove4j.sourceforge.net/javadocs/gnu/trove/map/hash/TByteFloatHashMap.html можно так
```scala
it should "check ..." in new Fixture {
  value.getDeepLayoutProbabilities(experiment, platform).keys()
    shouldBe Array[Byte](10, 7, 3)
}

it should "check ..." in new Fixture {
  value.getDeepLayoutProbabilities(experiment, platform).values()
    shouldBe Array(0.1f, 0.3f, 0.6f)
}
```

Проверить словари по ключам и значениям можно так
```scala
it should "..." in new Fixture {
  converter.convert(layoutConfigTest) should (
    contain key 0
      and contain value Map(
        "base" -> Map(10.toByte -> 0.1f, 5.toByte -> 0.8f, ...),
        "deep" -> Map(8.toByte -> 0.3f, 5.toByte -> 0.4f, ...),
      )
  )
}
```