Напишем простое Spark-приложение
```scala
// SimpleApp.scala
import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]) = {
    val logFile = "./logFile.txt"
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numsA = logData.filter(line => line.contains("a")).count()
    val numsB = logData.filter(line => line.contains("b")).count()
    println(s"==> Lines with a: $numsA, lines with b: $numsB")
    spark.stop()
  }
}
```

Для сборки проекта нужен файл сборки
```bash
# build.sbt
name := "SimpleApp"

version := "1.0"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided"
)
```

Установить утилиту `sbt` на Linux Manjaro можно так
```bash
$ sudo pacman -S sbt
```

Для того чтобы `sbt` работал корректно, требуется разместить `SimpleApp.scala` и `build.sbt` следующим образом:
- файл `build.sbt` должен лежать в корне проекта,
- а сценарий Scala -- по пути `src/main/scala/SimpleApp.scala`

Теперь можно упаковать приложение
```bash
# из корня проекта
$ sbt package
```
Для запуска Scala-приложения используется утилита `spark-submit`
```bash
$ spark-submit \
    --class "SimpleApp" \
    --master local \ 
    target/scala-2.12/simpleapp_2.12_1.0.jar
```

Запустить Python-приложение можно так
```bash
$ spark-submit \
    --master local
    ./simple_app.py
```