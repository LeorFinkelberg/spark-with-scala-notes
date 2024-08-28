Файл `*.scala` находится в директории 
``` bash
odnoklassnikik-ml-core/odnoklassniki-analisys/scala/odkl/analysis/spark/feed/channels/
```
Ремарка: На атрибутах case-класса входа (постфикс `Input`) в методе `run()` мы можем вызывать метод `.read(sparkSession)`, например
```scala
val showFeeds = executionContext.input.shownFeeds.read(sparkSession)
// или так
val sentFeeds = executionContext.input.sentFeeds.read(sparkSession).repartition(settings.sentFeedsPartitions)
```
а на атрибутах case-класса выхода (постфик `Output`) можно вызывать метод `.save(...)`, то есть (хотя в общем случае можно вызывать и `.read()`)
```scala
executionContext.output.attributedValues.save(...)
```
Сам файл
```scala
// ValueAttributionJob.scala

// путь в пакете отсчитывается от каталога /scala
package odkl.analysis.spark.feed.channels

import io.circe.generic.auto._
import odkl.analysis.spark.feed.channels.events._ // это просто директория со scala-файлами
import odkl.analysis.spark.job.ExecutionContextV2SparkJobApp
import odkl.analysis.spark.job.datasets.DatasetLocation
import one.conf.annotation.PropertyInfo
import one.conf.converter.EnabledIds
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable

trait ValueAttributionJobSettings extends ChannelSettings {
  @PropertyInfo(defaultValue = "127")
  def portletInsereterAuditPartitions: Int

  @PropertyInfo(defaultValue = "127")
  def sentFeedsPartitions: Int
  ...

  @PropertyInfo(defaultValue="content,vote,play,more,...")
  def valueTargets: Array[String]
  ...
}

// Для входных данных
case class ValueAttributionJobInput(
  feedPortletInserterAudit: DatasetLocation,
  sentFeeds: DatasetLocation,
  shownFeeds: DatasetLocation,
  clickedFeeds: DatasetLocation,
)

// Для выходных данных
case class ValueAttributionJobOutput(
  attributedValues: DatasetLocation
)

// имя объекта должно совпадать с именем scala-файла
object ValueAttributionJob extends ExecutionContextV2SparkJobApp[
  ValueAttributionJobInput,
  ValueAttributionJobOutput,
  ValueAttributionJobSettings,
] {
  override def run(executionContext: CurrentExecutionContext): Unit = {
    import sparkSession.implicits._

    val portletInserterAudit = executionContext.input.feedPorletInserterAudit.read(sparkSession)
  .repartition(settings.portletInserterAuditPartitions)
  }

    val sentFeeds = executionContext.input.sentFeeds.read(sparkSession)
      .repartition(settings.sentFeedsPartitions)
  }

    val shownFeeds = executionContext.input.shownFeeds.read(sparkSession)
    val clickedFeeds = executionContext.input.clickedFeeds.read(sparkSession)

    val unattributedSent = sparkSession.sparkContext.longAccumulator(Constants.send)
    val unattributedShown = sparkSession.sparkContext.longAccumulator(Constants.shown)
    val unattributedClicked = sparkSession.sparkContext.longAccumulator(Constants.clicked)

    executionContext.output.attributedValues.save(
      attribute( // функция; определена ниже
        porletInserterAudit,
        sentFeeds,
        shownFeeds,
        clickedFeeds,
        settings,
        unattributionSent,
        unattributionShown,
        unattributionClicked
      ).repartition(settings.outputPartitions).write
    )

    val (sent, shows, clicks) = executionContext.output.attributedValues.read(sparkSession)
      .agg(
        count(when(col(Field.sentTimestamp) > 0, col(Field.sentTimestamp))).as(Field.sent),
        sum(size(col(Field.shows))).as(Field.shows),
        sum(size(col(Field.clicks))).as(Field.clicks),
      )
      .as[(Long, Long, Long)]
      .collect()
      .head

    logWarning("Attribution stats:" +
      s" attributedSent=$sent," + 
      s" attributedShows=$shows," +
      s" attributedClicks=$clicks," +
      s" unattributedSent=${unattributedSent.value}," + 
	  s" unattributedShown=${unattributedShown.value}," +
	  s" unattributedClicked=${unattributedClicked.value},"
    )
  }
```
Ремарка: все что лежит в директории `odkl/analisys/spark/feed/channels/events/` как бы становится частью пространства имен того scala-файла, в котором выполняется инструкция. То есть все сущности (трейты, классы и пр.), которые лежат в scala-файлах этой директории будут доступны при импорте в манере
```scala
import odkl.analysis.spark.feed.channels.events._ 
```

Конструкция `sum(size(col(Field.shows)))` вычисляет количество элементов в каждой строке атрибута `Field.shows` (с помощью `size`), а затем складывает эти значения и потому на выходе получается общее количество элементов в атрибуте `Field.shows` (предполагается, что каждое значение атрибута `Field.shows` - это коллекция).

В методе `run()` значение поля `feedPorletInserterAudit` (см. case-класс `ValueAttributionJobInput`) из входного набора можно "дернуть" так
```scala
executionContext.input.feedPorletInserterAudit.read(sparkSession)
```
а значение `portletInserterAuditPartitions` дергаем из трейта `ValueAttribution`
В директории 
``` bash
odnoklassnikik-ml-core/odnoklassniki-analisys/scala/odkl/analysis/spark/feed/channels/events
```
лежит scala-файл, описывающий трейт `ChannelSettings`
```scala
// ChannelSettings.scala

package odkl.analysis.spark.feed.channels.events

import one.conf.annotation.PropertyInfo

trait ChannelSettins {
  @PropertyInfo(defaultValue = "127,129,130")
  def bannerFeedTypes: Array[Short]

  @PropertyInfo(defaultValue = "131")
  def portletFeedTypes: Array[Short]

  @PropertyInfo(defaultValue = "132,487,999")
  def excludedFeedTypes: Array[Short]

  ...
}
```