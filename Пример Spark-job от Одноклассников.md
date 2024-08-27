Файл `*.scala` находится в директории 
``` bash
odnoklassnikik-ml-core/odnoklassniki-analisys/scala/odkl/analysis/spark/feed/channels/
```
Сам файл
```scala
// ValueAttributionJob.scala

// путь в пакете отсчитывается от каталога /scala
package odkl.analysis.spark.feed.channels

import io.circe.generic.auto._
import odkl.analysis.spark.feed.channels.events._ // это просто директория со scala-файлами
import odkl.analysis.spark.job.ExecutionContextV2SparkJobApp
...
```