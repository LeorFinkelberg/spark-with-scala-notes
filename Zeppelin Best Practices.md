### Создать директорию в Zeppeline

В ячейках пишем
```bash
%md
_Description ..._

%sh
echo HADOOP_HOME  # /opt/hadoop

%sh
$HADOOP_HOME/bin/hdfs dfs -mkdir /user/alexander.podvoyskiy/data
```
### Настройка интерпретатора

Рассмотрим важные с точки зрения эффективного функционирования Zeppelin (и Spark в целом) настройки:
- `spark.app.name`: с точки зрения именования важны следующие аспекты:
	- Zeppelin должен убиваться каждую ночь, поэтому название должно начинаться с Zeppelin,
	- Коллеги должны иметь возможность разобраться откуда Zeppelin запущен, поэтому хорошим тоном считается указать хост и порт в имени, например Zeppelin_srve957_4059
- `spark.default.parallelism`: _Если явно не задан, то вычисляется как общее количество ядер в кластере_, ==что может приводить к излишне длинным чтениям в ситуации большого числа файлов из-за малого параллелизма на старте или, наоборото, из-за слишком большого параллелизма при чтении небольшой паркетины после тяжелого расчета==. Эмпирически в Одноклассниках установлено _количество партиций 16_.
- `spark.dynamicAllocation`: _динамическая система выделения ресурсов в зависимости от имеющихся задач, must have  для работы с интерактивными инструментами на большом кластере_, поэтому разберем подробнее:
	- `enabled`: включена или нет. Имеет смысл отключать, как правило, только для запуска стриминга или машинного обучения с заранее рассчитанным фиксированным количеством ресурсов.
	- `minExecutors`: минимальное число исполнителей, ниже которого не опускаться. Наличие готовых исполнителей может заметно улучшить скорость ответа, но приведет к уменьшению емкости кластера. Рекоммендуется резервировать _не более 4-6 ядер_.
	- `maxExecutors`: больше не значит лучше, так как с какого-то момента начинает тупить и глючить драйвер. Рекомендуется ставить _не более 512_.
	- `executorIdleTimeout`: насколько быстро освободившись исполнители возвращаются в кластер. Могут возникнуть проблемы с большим количеством маленьких задач при низких значениях параметра. Рекомендуется ставить 60 секунд.
- `spark.executor.instances`: включение явного количества исполнителей отключает динамическое выделение. Используем только стриминга и ML
- `spark.executor.memory/cores`: количество ресурсов под один исполнитель -- слишком жирные сложно алоцировать, слишком тощими сложно управлять. Рекомендуется 16Gb на 4 ядра.
- `spark.sql.shuffle.partition`: параллелизм при обработке перетасовок в SQL; выбираем простые числа, рекомендуется 511. Перед сохранением кадра данных _обязательно уменьшить число партиций до разумного_ используя `.repartition` (целимся в +/- 128 Мб на партицию, лучше немного недобить). Если нужно сделать параллелизм для какой-то операции больше, то `.repartition` перед `.groupBy` поможет и здесь.
- `zeppelin.interpreter.output.limit`: ограничивает максимальный размер в байтах для ответа интерпретатора в параграфе. Обеспечивает "защиту от дурака", так как при попытке отрисовать слишком много UI начинает тупить. Не рекомендуется менять дефолт 102400 без осознания последствий.
- `zeppelin.spark.maxResult`: похоже на `zeppelin.interpreter.output.limit`, но в записях + помимо рендеринга влияет на то, сколько данных осядет в памяти интерпретатора. Опытные пользователи могут поднять до 10000 и принять ответственность, новичкам лучше остаться на дефолте 1000.
- `zeppelin.spark.useHiveContext`: создавать ли по дефолту контекст Hive (функциональнее), или нативный (быстрее). Многие выбирают контекст Hive ради функций типа `.collect_list`, но с недавних пор они доступны в наше классе `SQLOperations` и для нативного контекста. Рекомендованное значение `false`.
- `fs.defaultFS/spark.hadoop.yarn.resourcemanager.cluster-id/spark.hadoop.yarn.resourcemanager.ha.rm-ids`: позволяют переключать интерпретатор между кластерами. По умолчанию идет с datalab-hadoop, но иногда полезно подключать и datalab-hadoop-backup, выставив `hdfs://datalab-hadoop-backup/datalab-hadoop-backup-rm/servd2135,srvg671`,
- `spark.sql.partquet.mergeSchema`: полезный параметр, позволяющий проще читать паркеты пачкой при небольших различиях в схеме. По дефолту у всех включен.
- `spark.yearn.queue`: очередь, в которой выполняются контейнеры Spark. Есть 3 варианта:
	- `root.half`: выжирает половину ресурсов кластера, оставляя пространство проду и другим задачам. Рекомендовано для Zeppelin по умолчанию
	- `root.rootDefault`: выжирает до 85% кластера. Используем для многопользовательских Zeppelin. Индивидуальное использование должно быть осознанным.
	- `root.rootQueue`: ==за неосознанное использование ждут суровые санкции! Не надо использовать!==
- `spark.yarn.executor.memoryOverhead`: поскольку Spark баглив и до памяти на шафлы жаден, в отведенный лимит часто не укладывается. Если видны убийства контейнеров YARN по памяти, можно попробовать поднять с дефолтных 3584 до больших значений.
- `spark.executorEnv.JAVA_HOME/spark.yearn.appMasterEnv.JAVA_HOME`: какую Java использовать для запуска Spark.
### Как не надо делать

- ==Не следует злоупотреблять кэшированием (`.cache` / `.persist`)==,
	- Так как расходуется ценный ресурс памяти, шафлы становятся медлнее, YARN  чаще убивает испольнителей.
	- Ухудшается локальность, так как вместо 3 нод держащих HDFS-реплики в работе 1 одна будет с кэш.
	- Отключаются паркетные оптимизации по сжатию и индексации, теряются преимущества колоночного формата.
	- При отключении исполнителя (по таймауту dynamic allocation или из-за сбоя) кэш приходится перерассчитывать
	- Не влезающие в память блоки сбрасываются на диск, что может привести к выпадению нод из кластера по дискам
	- Best practics кэширования:
		- Сохранение во временный паркет в 9 случаях из 10 работает лучше
		- Кэшировать есть смысл при работе с ML-алгоритмами, рассчитав число партиций, реплик и минимизировав число операций над кэшированной частью
		- Еще один случай -- кэшировать небольшой блок (до нескольких сотен Мб и не более 4-х партиций), прочитанный из паркета для того чтобы подергать ad-hook запросы
- ==Метод `.collect` используем очень осторожно==. А то можно быстро исчерпать память на драйвере и он зависнет. На `.collect` в коде не работают лимиты Zeppelin и он вас не спасет. Как правильно использовать `.collect`:
	- Собираем на драйвере только небольшие наборы, в конечных размерах которых мы уверены.
	- По возможности собираем примитивы (они придут массивом и займут мало места)
	- Используем метод `.limit` для подстраховки, если не уверены
	- Проверяем методом `.count` перед сборкой, если не знаем сколько записей
	- Используем `.show` / `.take` / `z.show` для того, чтобы посмотреть пример.
- ==Нельзя забывать делать `.repartition`  перед `write` / `cache` / `collect`==, что приводит к большому числу маленьких кусочков, которые тормозят все в дальнейшем. Ориентируемся на 128 Мб на файл при сохранении.
- ==Нельзя забывать отфильтровывать NULL-ключи==. Работает и для других дефолтных значений (например, `""`, `0`) . Почти не влияет на операции с хорошим комбайном (`sum` / `count`); проверить есть ли проблема можно так `groupBy($"key").count.orderBy($"count".desc).show(100)`. ==Для операций без хорошего комбайна (`.countDistinct`) становится фатальным== -- _фильтруем заранее_.
- ==Нельзя считать сразу на всей истории / месяце / неделе / дне (зависит от масштаба)== -- вероятность того, что написанный код с первого раза отработает правильно близка к нулю, а время и ресурсы кластера потеряно.
	- Сначала обкатываем код на фрагменте, где исполнение занимает не более 1 минуты (например, один день или даже один парт этого дня, а может предварительно сохраненный sample),
	- Проверяем, что результат соответствует ожиданиям.
	- Масштабируя код старайтесь не запускать задач более чем на 10-15 минут -- при необходимости сделайте цикл `for` по датам и обрабатывайте под дню/неделе/месяцу за раз с сохранением в паркет

Обязательно нужно двигаться постепенно, с сохранением промежуточных результатов. Нужно валидировать промежуточные результаты, обкатывать на подмножестве данных. Не забываем удалять временные файлы.
### Читаем и пишем данные

Самый простой вариант: `sqlContext.read.parquet("")`, но есть нюансы:
- Есть разные варианты задания дат, за которые нужно читать:
	- В простом случае используем `glob`: `sqlContext.read.parquet("/kafka/parquet/idsContentActions/date=2017-11-{19,2*}`,
	- В сложных случаях используем `DataRange.readParquet`: `DateRange("30 days ago: today).readParquet(sqlContext, "/kafka/parquet/feedbackMails")`
- Если возникают ошибки при чтении несовместимых схем (и `spark.sql.parquet.mergeSchema` не помог) или ошибки на логические типы Hive(`TIMESTAMP_MS`), то схему специфицируем в ручную
- Для того чтобы не потерять `date` , не забываем добавить `.option("basePath", folder)`
- Чтобы разложить выход по папкам (аналогично `date`), используем `write.partitionBy("date")`. ==Имеем в виду, что `repartition($"date").write` и `write.partitionBy("date")` это совсем разные вещи==.
- Не забываем при записи правильно выбрать число итоговых партиций (==дефолт почти всегда будет провален!==)
- На производительность чтения сильно влияет дефолтный параллелизм.

`SequenceFile` читаем, используя не `sqlContext`, а базовый `sc`, не забывая про то, что нужно ВСЕГДА преобразовывать то, что прочитали в другой формат (тапл, кейс класс, строку) перед тем, как делать шафл. Использование `case class` -- самый простой способ перейти к `parquet`, просто сделав `.toDF`
```scala
case class MetadataRecord(
  objectType: String,
  id: Long,
  appId: Long
)

val metadata = sc
  .sequenceFile[ObjectId, ObjectMetadata]("/kafka/metadata/current")
  .filter(_._2.getOwnerType == OwnerType.USER_APP)
  .map(x => MetadataRecord(x._1.getType.name, x._1.getId1, x._2.getApplication))
  .toDF
```

Читаем сырые логи
```scala
import org.apache.hadoop.io.NullWritable
import odkl.analysis.job.kafka.MessageWritable

sqlContext
    .read
    // если задать .schema(...), будет в разы быстрее
    .json(sc.sequenceFile[NullWritable, MessageWritable](s"kafka/published/20161222*/shownFeeds72").map(_._2.toString))
    .where("recipient = 531455...")
    .repartition(1)  // ??? Почему не .coalesce()?
    .write.parquet(s"feeds/shownParquet")
```

CSV-формат чаще всего используется для перегонки данных в Python или отдачи вовне. Выглядеть это может примерно так
```scala
dataframe
  .repartition(1)  // ??? Почему не .coalesce()?
  .write.format("com.databricks.spark.csv")
  .option("header", "true")
  .save("path")
```

Как и все Hadoop-ие выводы результат будет иметь имя `part_много_цифр`, но его легко выкачать локально и назвать как нужно.
```scala
val clusters = sqlc.read.parquet(s"${settings.clustersPath}/date=$endDate")
val categories = JavaConversion.mapAsScalaMap(settings.categoriesMap)

val fs = FileSystem.get(sc.hadoopConfiguration)

try {
  categories.foreach {
    val category = x._1
    val filter = JavaConversions.asScalaSet(x._2)

    clusters
      .where(s"cluster IN (${filter.mkString(",")})")
      .select("userId")
      .repartition(1)
      .sortWithinPartitions($"userId")
      .write
      .mode("overwrite")
      .format("com.databricks.spark.csv")
      .save(s"${settings.temp}/exporter/$category")

    logInfo(s"Exporting category $category ...")

    fs.rename(
       new Path(s"${settings.temp}/exporter/$category/part-00000"),
       new Path(s"${settings.categoriesImporterPath}/$category.csv")
    )

    logInfo(s"Exporting category $category done.")
  }
} finally {
  fs.close()
}
```

С Avro мы чаще всего сталкиваемся при работе с данными в DWH-Hadoop. Из коробки Avro-формат не работает из-за того, что у файлов там нет расширения Avro, однако это относительно просто исправить
```scala
sc.hadoopConfiguration.set("avro.mapred.ignore.inputs.without.extenxion", "false")

val data = sqlContext.read
  .format("com.databricks.spark.avro")
  .load("hdfs://datalab-hadoop-dwh-stable/data/ok/CustomersConnections/v20170607/*")
```

ORC тоже еще встречается в DWH-Hadoop (это нативный формат для Hive). Для чтения можно использовать специальный формат
```scala
val data = sqlContext.read
  .format("com.databricks.spark.avro")
  .load("hdfs://datalab-hadoop-dwh-stable/data/ok/CustomersConnections/v20170607/*")
```

Или использовать встроенный лоадер, инициализирующий Hive (Hive лучше избегать)
```scala
val oldOrc = sqlContext.read.orc("hdfs://srvd2852:8020/data/ok/Photos/v20180726")
```
### Делаем бродкасты

Часто возникает необходимость пофильтровать данные по набору ID, разрезолвить число ID в текст и т.д. В этом случае задача может пройти значительно быстрее при использовании map-side операций, предотвращающих или значительно уменьшающих размер шафла. В зависимости от ситуации могут быть разные способы этого добиться:
- Фильтр `A in ()` непосредственно в SQL запросе нормально  работает, ==если количество ID не превышает сотен ($\approx 100$)==
```scala
data.where(s"id in (${ids.mkString(",")}")
```
- Если ID-ников доходит до нескольких десятков тысяч ($\approx 10000$), то можно использовать простую UDF
```scala
val filter = sqlContext.udf.register("myIds", (x: Long) => ids.contains(x))
```
- Если же количество ID измеряется от сотен тысяч до сотен миллионов ($\approx 100 \cdot e^3 \ldots 100 \cdot e^6$), то используем броадкаст с сортированным массивом
```scala
val ids = sc.broadcast(likes.select("objectId").distinct().repartition(1).sortWithinPartitinos("objectId").map(_.getLong(0)).collect)

val wereLiked = sqlContext.udf.register("wereLiked", (x: Long) => java.util.Arrays.binarySearch(ids.value, x) >= 0)
```
- В случае, если фильтра не достаточно, а нужен join, можно попросить Spark сделать broadcast самому (работает на сотнях тысяч, но не на десятках миллионов)
```scala
val sportDocuments = texts
  .where("text IS NOT NULL")
  .select(
    sportTokenize($"text").as("sport"),
    $"id".as("DOCUMENT_D"))
  )
  .where("SIZE(sport) > 0")

likes
  .join(functions.broadcast(sportDocuments), Seq("DOCUMENT_ID"))
  .repartition(1)
  .write.parquet(s"$basePath/sportProfiles")
```
Но если нужно сделать join на более крупный кусок, где Spark не справляется, то это реализуемо через два broadcast с синхронизированным порядком
```scala
val sorted = sqlc.read.parquet(categoriesInput).reparatition(1).sortWithinPartitions("userId").cache
val user = sc.broadcast(sorted.map(_.getLong(0)).collect())
val categories = sc.broadcast(sorted.map(_.getInt(1).toByte).collect())

sorted.unpersist()

val category = functions.udf((x: Long) => {
  val index = java.util.Arrays.binarySearch(users.value, x)
  if (index >= 0) {
    categories.value(index)
  } else {
    -1.toByte
  }
})
```
NB! Spark в некоторых случаях может сделать broadcast и сам (в соответствии с параметром `spark.sql.conf.autoBroadcastJoinThreshold`), но доверять ему не следует -- лимит настроен на маленькое значение и поднимать не рекомендуется (используйте `functions.broadcast`)