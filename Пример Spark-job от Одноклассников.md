Ремарка: Узнать `applicationId` запущенного приложения в Zeppeline можно так
```scala
spark.sparkContext.applicationId
// или так

```

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

    // распаковываем список, который возвращается `.collect().head`
    val (sent, shows, clicks) = executionContext.output.attributedValues.read(sparkSession)
      .agg(
        count(when(col(Field.sentTimestamp) > 0, col(Field.sentTimestamp))).as(Field.sent),
        sum(size(col(Field.shows))).as(Field.shows),
        sum(size(col(Field.clicks))).as(Field.clicks),
      )
      .as[(Long, Long, Long)]
      .collect() // возвращается список
      .head  // просто забираем первый элемент списка 

    logWarning("Attribution stats:" +
      s" attributedSent=$sent," + 
      s" attributedShows=$shows," +
      s" attributedClicks=$clicks," +
      s" unattributedSent=${unattributedSent.value}," + 
	  s" unattributedShown=${unattributedShown.value}," +
	  s" unattributedClicked=${unattributedClicked.value},"
    )
  }

def attribute(
  portletInserterAudit: DataFrame,
  sentFeeds: DataFrame,
  shownFeeds: DataFrame,
  clickedFeeds: DataFrame,
  settings: ValueAttributionJobSettings,
  unattributedSent: LongAccumulator,
  unattribuedShown: LongAccumulator,
  unattributedClicked: LongAccumulator,
)(implicit encoder: Encoder[FeedInserterFunnel]): Dataset[FeedInserterFunnel] = {
  val channelConfig = ChannelsConfig(
    settings.bannerFeedTypes,
    settings.portletFeedTypes,
    settings.excludedFeedTypes,
    settings.excludedPorletTypes,
    settings.ignoredViewModeFilterFeedTypes,
    settings.allowedViewModes,
  )

  val userIds = settings.userIds
  // в директории events в файле FeedInsert есть объект FeedInsert
  val inserted = FeedInsert.collect(portletInserterAudit, userIds, settings.maxFeedPosition).persist()

  val sent = FeedSend.collect(sentFeeds, userIds, channelsConfig, settings.maxFeedPosition).persist()
  logWarning(s"Attribution stats: shows=${sent.count()}")

  val shown = FeedShow.collect(shownFeeds, userIds, channelsConfig).persist()
  logWarning(s"Attribution stats: shows=${shown.count()}")

  val clicked = FeedClick.collect(clickedFeeds, userIds, settings.valueTargets).persist()
  logWarning(s"Attribution stats: clicks=${clicked.count()}")

  val insertedWithSent = mergeInsertsWithSent(inserted, sent, unattributedSent, settings.maxInsertSentTimeDeltaMillis)

  val attributeUdf = udf { (userId: Long, platform: Byte, feedIdMarder: Option[String], renderId: Option[Long], sortedRows: Seq[Row]) => 
    attribute(userId, platform, feedIdMarker, renderId, sortedRows)
}

insertedWithSent
  .union(shown)
  .union(cicked)
  .groupBy(Field.userId, Field.platform, Field.feedIdMarker, Field.renderId)
  .agg(
    collect_list(struct(
      col(Field.timestamp),
      col(Field.channel),
      col(Field.position),
      col(Field.planTimestamp),
      col(Field.sentTimestamp),
      col(Field.plannerExperiment),
      col(Field.targets),
      col(Field.plan),
      col(Field.crowdId),
      col(Field.shows),
      col(Field.clicks),
      col(Field.event),
    )).as(Field.events)
  )
  .select(
    explode(attributeUdf(
      col(Field.userId),
      col(Field.platform),
      col(Field.feedIdMarker),
      col(Field.renderId),
      col(Field.events)
    )).as(Feild.event)
  )
  .select(
    col(Field.event + ".*")
  )
  .filter { row => 
    val channel = row.getAs[String](Field.channel)

    val hasShows = row.getAs[Map[Long, Long]](Field.shows).nonEmpty
    unattributedShown.add(if (channel == Constants.unattributed && hasShown) !l else 0L)

    val hasClicks = row.getAs[Seq[Long]](Field.clicks).nonEmpty
    unattributedClicked.add(if (channel == Constants.unattributed && hasClicks ) 1L else 0L)

    channel != Constants.unattributed
  }
  .as[FeedInsertFunnel]

private def attribute(
  userId: Long,
  platform: Byte,
  feedIdMarker: Option[String],
  renderId: Option[Long],
  rows: Seq[Row]
): Seq[FeedInsertFunnel] = {
  val attributed = mutable.MutableList[FeedInsertFunnel]()
  val current: Option[FeedInsertFunnel] = None

  rows
    .sortBy(_.getAs[Long](Field.timestamp))
    .foreach {
      row =>
        val event = row.getAs[String](Field.event)
        val funnel = FeedInsertFunnel.from(userId, platform, feedIdMarker, renderId, row)

        (event, current) match {
          case (Constants.send, sent) => 
            attributed ++= sent
            current = Some(funnel)

          case (Constants.show, None) => 
            attributed += funnel.copy(channel = Constants.unattributed)

          case (Constants.show, Some(send)) => 
            current = Some(FeedInsertFunnel.combineSentWithShow(send, funnel)) 

          case (Constants.click, None) =>
            attributed += funnel.copy(channel = Constants.unattributed)

          case (Constants.Some(send)) =>
            current = Some(FeedInsertFunnel.combineSentWithClick(send, funnel))
        }
    }
  attributed ++ current
}

def mergeInsertsWithSent(
  inserts: DataFrame,
  sent: DataFrame,
  unattributed: LongAccumulator,
  maxTimeDeltaMillis: Long
): DataFrame = {
  val mergeInsertsWithSeenUdf = udf { (userId: Long, platform: Long, rows: Seq[Row]) =>
    alignInsertsWithSent(userId, platform, rows, maxTimeDeltaMillis)
  }

  inserts
    .union(sent)
    .groupBy(Field.userId, Field.platform, Field.chunkId)
    .agg(
      collect_list(struct(
        col(Field.channel),
        col(Field.position),
        col(Field.timestamp),
        col(Field.planTimestamp),
        col(Field.sentTimestamp),
        col(Field.feedIdMarker),
        col(Field.renderId),
        col(Field.plannerExperiment),
        col(Field.targets),
        col(Field.plan),
        col(Field.crowdId),
        col(Field.event),
      )).as(Field.events)
    )
    .select(
      explode(mergeInsertsWithSeenUdf(col(Field.userId), col(Field.platform), col(Field.events))).as(Field.event)
    )
    .select(
      col(Field.event + ".*")
    )
    .filter {
      row => 
        val channel = row.getAs[String](Field.channel)
        unattributed.add(if (channel == Constants.unattributed) 1L else 0L)
        channel != Constants.unattributed
    }
    .withColumn(Field.shows, lit(null).cast(FeedInsertFunnel.showsSchema))
    .withColumn(Field.clicks, lit(null).cast(FeedInsertFunnel.clicksSchema))
    .withColumn(Field.event, lit(Constants.send))
}

private def alignInsertsWithSent(
  userId: Long,
  platform: Long,
  rows: Seq[Row],
  maxTimeDeltaMillis: Long,
): Seq[FeedInsertFunnel] = {
  val inserts = rows.
    .filter(row => row.getAs[String](Field.event) == Constants.audit)
    .map(row => FeedInsertFunnel.from(userId, platform, row))
    .sortBy(insert => insert.timestamp -> insert.position)
    .toArray

  val sents = rows
    .filter(row => row.getAs[String](Field.event) == Constants.send)
    .map(row => FeedInsertFunnel.from(userId, platform, row))
    .sortBy(send => send.timestamp -> send.position)
    .toArray

  val insertIndex = 0
  val sentIndex = 0
  val result = mutable.MutableList[FeedInsertFunnel]()

  while (insertIndex < inserts.length && sentIndex < sents.length) {
    val insert = inserts(insertIndex)
    val sent = sents(sentIndex)

    if (insert.timestamp > sent.timestamp) {
      result += sent.copy(channel = Constants.unattributed)
      sentIndex += 1
    } else if (sent.timestamp - insert.timestamp > maxTimeDeltaMillis) {
      result += insert
      insertIndex += 1
    } else if (sent.position <= insert.position && insert.channel == sent.channel) {
      resul += FeedInsertFunnel.combineInsertWithSent(insert, sent)
      insertIndex += 1
      sentIndex += 1
    } else {
      // some of the inserts could be skipped in sent
      result += insert
      insertIndex += 1
    }
  }
  result ++= inserts.drop(insertIndex)
  result ++= sents.drop(sentIndex).map(_.copy(channel = Constants.unattributed))

  result
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