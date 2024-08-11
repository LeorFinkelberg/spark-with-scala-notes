Порядок установки [[Литература#^008892]]<c. 553>:
- Переходим в директорию `/opt`
```bash
$ cd /opt
```
- Со страницы проекта https://spark.apache.org/downloads.html скачиваем tar-архив, например, `spark-3.5.1-bin-hadoop3.tgz`
```bash
$ sudo wget https://www.apache.org/dyn/closer.lua/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz 
```
После скачивания в текущей директории появится файл с именем `spark-3.5.1-bin-hadoop3.tgz`.
- Распаковываем
```bash
$ sudo tar xvzf spark-3.5.1-bin-hadoop3.tgz
```
Для программных продуктов Apache рекомендуется давать имена с прификсом `apache-`, поэтому можно выполнить следующую команду
```bash
$ sudo mv {,apache-}spark-3.5.1-bin-hadoop3.tgz
```
- Чтобы без проблем устанавливать несколько версий одного программного продукта в системе, рекомендуется создать символическую ссылку (это что-то вроде ярлыка в Windows) на соответствующий файл
```bash
$ sudo ln -s apache-spark-3.5.1-bin-hadoop3 apache-spark
```
- В конце процесса установки -- очистка
```bash
$ sudo rm -f spark-3.5.1-bin-hadoop3.tgz
```
- И чтобы доступ к `spark-shell` можно было получить из любой точки системы, обновим переменную `PATH`
```bash
# .zshrc
...
export PATH=${PATH}:/opt/apache-spark/bin
```

Проверим работоспособность
```bash
$ spark-shell  # загрузится сессия
```