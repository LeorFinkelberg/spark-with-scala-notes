Для создания набора данных нам сначала нужно создать case-класс (класс-образец). Case-класс в Scala это что-то вроде дата-классов или именновых кортежей в Python
```scala
case class FootballTeam(
  name: String,
  league: String,
  matches_played: Int,
  goals_this_season: Int,
  top_goal_scorer: String,
  wins: Int
)
```
Создадим экземпляр case-класса
```scala
val brighton = FootballTeam(
  "Brighton and Hove Albion",
  "Premier League",
  matches_played = 29,
  gaols_this_season = 32,
  top_goal_scorer = "Neil Maupay",
  wins = 6
)
```

В Python мы могли написать
```python
import typing as t

class FootballTeam(t.NamedTuple):
  name: str
  league: str
  matches_played: int
  goals_this_season: int
  top_goal_scorer: str
  wins: int

brighton = FootballTeam(
  "Brighton and Hove Albion",
  "Premier League",
  matches_played = 29,
  gaols_this_season = 32,
  top_goal_scorer = "Neil Maupay",
  wins = 6,
)
```

Теперь создадим набор данных
```scala
val teams: Dataset[FootballTeam] = spark.createDataFrame(
  Seq(brighton, manCity, ...)
).as[FootballTeam]
```

А в Python так
```python
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

teams: SparkDataFrame = spark.createDataFrame([brighton, manCity, ...]) 
```

Чтобы добавить, например, новый столбец с константным значением в `Dataset`, следует
```scala
import org.apache.spark.sql.functions.lit

val newTeams = teams.withColumn("sport", lit("football"))
```

А в PySpark можно сделать так
```python
from pyspark.sql import functions as F

new_teams = teams.withColumn("sport", F.lit("football"))
```

Пользовательскую функцию можно определить следующим образом
```scala
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.expressions.UserDefinedFunction

// Чтобы сделать доступными все функции подмодуля `functions`, можно последний слот заменить на `_`
// import org.apache.spark.sql.functions._

def isPrem(league: String): Boolean = {
  league match {
    case "Premier League" => true
    case _ => false
  }
}

// Регистрируем UDF
val isPremUDF: UserDefinedFunction = udf[Boolean, String](isPrem)

// Применяем UDF
val teamsWithLeague: DataFrame = teams.withColumn( // Получился DataFrame!!!
  "premier_league",
  isPremUDF(col("league"))
)
```

Теперь, когда мы добавили новый столбец, не находящийся в нашем case-классе, на выходе получается `DataFrame`. Поэтому нужно либо добавить еще одно поле в изначальный case-класс, либо создать новый.

Можно вычислить агрегаты на наборе данных
```scala
teams.agg(
  Map(  // неизменяемый ассоциативный массив
    "matches_played" => "avg", 
    "goals_this_season" => "count" 
  )
).show()
```

А в PySpark так
```python
teams.agg({
    "matches_played": "avg", 
    "goals_this_season": "count" 
}).show()
```

Получить массив имен команд можно так
```scala
val teamNames: Array[String] = teams.map(_.name).collect()
// Это тоже самое что и
val teamNames: Array[String] = teams.map(team => team.name).collect()
```

Чтобы отфильтровать набор данных, опираясь на то, существует ли он в этом массиве, нам нужно обработать его как последовательность аргументов, вызвав `_*`
```scala
val filteredPlayers: Dataset[Player] = players.filter(col("team").isin(teamNames: _*))
```

Пару замечаний по настройке проекта. Для проекта нужен файл `build.sbt` с зависимостями проекта
```bash
# build.sbt
name := "spark-template"
version := "0.1"
scalaVersion := "2.12.11"
val sparkVersion = "2.4.3"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
```

Обертку для `SparkSession` можно оформить так
```scala
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("spark-example")
    .getOrCreate()
}
```

Теперь можно наследоваться от этого типажа
```scala
object RunMyCode extends SparkSessionWrapper {
  // код
}
```