В ячейке Zeppelin для того чтобы вывести содержимое директории с помощью `dfs` можно сделать так 
```bash
%sh
$HADOOP_HOME/bin/hdfs dfs -ls /kafka/parquet/feedPortletInserterAudit72/
```

Для того чтобы найти число партиций, на которое был разбит какой-то день, можно использовать конвейер
```bash
%sh
$HADOOP_HOME/bin/hdfs dfs -ls /kafka/parquet/feedPortletInserterAudit72/date=2024-08-26 | wc -l
# 378 партиций
```

Размер партиции можно оценить так
```bash
%sh
$HADOOP_HOME/bin/hdfs dfs -du -s -h /kafka/parquet/feedPortletInserterAudit72/date=2024-08-26/part-0000-1918b993-...c000.gz.parquet
# 173.1 M 519.4 M /kafka/parquet/...
```
