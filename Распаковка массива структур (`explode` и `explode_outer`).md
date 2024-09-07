Если схема кадра данных показывает, что атрибут имеет тип массива структур, то есть что-то вроде
```bash
|-- corwId: map(nullable = true)
|    |-- key: string
|    |-- value: integer = true)
|-- shows: array (nullable = true)  # <- NB!
|     |-- element: struct (containsNull = true)
|          |-- timestamp: long (nullable = true)
|          |-- showMillis: long (nullable = true)
|-- clicks: array (nullable = true)
|    |-- element: struct (containsNull = true)
|    |    |-- timestamp: long (nullable = true)
```
то развернуть элементы массива в столбцы можно так
```scala
import org.apache.spark.sql.functions._

df.withColumn(
  "shows",
  explode(col("shows"))  // NB!
)
.select(
  "*",
  "shows.*"  // Передать нужно обязательно строку, а не Col-объект!!!
)
```

Тогда на выходе получим таблицу с таким заголовком
```bash
userId | platform | ... | shows | clicks | timestamp | showMillis |
```

Пример 
```scala
data
  .drop("timestamp")  // после распаковки будет еще несколько "timestamp"
  .withColumn("shows", explode_outer(col("shows")))
  .select("*", "shows.*")
  .withColumnRenamed("timestamp", "showTimestamp")
  .withColumn("clicks", explode_outer(col("clicks")))
  .select("*", "clicks.*")
  .withColumnRenamed("timestamp", "clickTimestamp")
  .groupBy("userId", "platform").agg(
    collect_set(col("channel")) as "channel",
    max(col("position")) as "maxPosition",
    count(col("position")) as "groupPower",
    count(col("clickTimestamp")) as "countClickTimestamp",
    mean("showMillis") as "meanShowMillis",
    stddev("showMillis") as "stdShowMillis",
    count(col("showMillis")) as "countShowMillis"
  ).show(5, 300, true)
```