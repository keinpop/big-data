# Как запустить
!!! Запускаем все из корневой папки проекта 
## 1. Запустим контейнеры с постгресом, спарком, кликхаусом и монгой. 
P.S. Хотел попробовать еще Касандру, но к сожалению у меня почему-то лагает запуск с ней, решил не рисковать.
```bash
docker compose up -d
```
Помимо всех необходимых контейнеров, у нас запустить sql-скрипт, который считает данные и загрузит, для дальнейшей работы
## 2. Запустим скрипт на Spark, который по аналогии с первой лабораторной работой перекладывает исходные данные из PostgreSQL в модель звезда в PostgreSQL.

```bash
docker exec -it spark-master spark-submit \                                                                           
  --master spark://spark-master:7077 \  
  --deploy-mode client \  
  --jars /opt/jars/postgresql-42.6.0.jar \
  /opt/spark-apps/tmp_to_star_schema.py
```
После выполнения скрипта, а так же получения КУУУУЧИ логов уровня INFO, важно найти строчку:
```bash
25/05/15 19:28:47 INFO SparkContext: Successfully stopped SparkContext
```
которая говорит нам о том, что все окей произошло и наш `spark.stop()` отработал корректно.

## 3. Запустим скрипт на Spark, который создаёт все 6 перечисленных (в ТЗ) отчетов в виде 6 отдельных таблиц в ClickHouse.

```bash
docker exec -it spark-master \
  spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --jars "/opt/jars/postgresql-42.6.0.jar,/opt/jars/clickhouse-jdbc-0.4.6.jar" \
    --driver-class-path "/opt/jars/postgresql-42.6.0.jar:/opt/jars/clickhouse-jdbc-0.4.6.jar" \
    /opt/spark-apps/clickhouse.py
```
После запуска скрипта, нужно убедиться в том, что все окей и данные на месте. Запустите данную команду, чтобы убедиться:
```bash
docker exec -it lab2-clickhouse-1 \
  clickhouse-client --query "SELECT * FROM avg_check_by_customer LIMIT 10 FORMAT PrettyCompact"
```
В целом, вы можете ввести на место `avg_check_by_customer` любую табличку (чтобы получить список таблиц `docker exec -it lab2-clickhouse-1 clickhouse-client --query "SHOW TABLES"`) и проверить, что все окей.

## 4. Запустим скрипт на Spark, который создаёт все 6 перечисленных (в ТЗ) отчетов в виде 6 отдельных коллекций в MongoDB.
``` bash
docker exec -it spark-master spark-submit \
 --master spark://spark-master:7077 --jars /opt/jars/postgresql-42.6.0.jar \
 --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 \
 /opt/spark-apps/mongo.py
```
Здесь, чтобы убедиться, что все ок. Написал минискриптик на питоне, который проверит данные. 
P.S. Запускать из корня проекта:
```bash
python3 scripts/mongo_check.py 
```