### Задание (job) зависло

Задание зависло -- активных задач (task) нет, но вычисления не идут.

Нужно проверить Task Deserialization Time и/или убрать трансляцию (broadcast) с больших наборов данных.
### Тормозит на шаге map

Использовать `.mapPartiations()`. Пример
```scala
// было
.map {
  x => 
  val conn = new Connection()
  conn.close()
}

// стало
.mapPartitions {
  xs =>
    val conn = new Connection()
    for (x <- xs) {
      ... 
    }
    conn.close()
}
```
### Проблемы сериализации

Симптом: Task not serializable: java.io.NotSerializableException. Как лечить: в stack-trace будет указан элемент, который не удалось сериализовать. Этот элемент нужно создавать на рабочих узлах, а не на драйвере. Например, внутри `.mapPartitions()`.

Или другой симптом: WARN TaskSetManager: Stage 1 contains a task of very large size (100500 KB). The maximum recommended task size is 100 KB. Как лечить: убрать большие объекты из замыкания (создавать большие объекты на рабочих узлах).

