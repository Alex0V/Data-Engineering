import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, to_date, count, desc, year, month, row_number, expr
from pyspark.sql.window import Window


# середня тривалість поїздок на день
def avg_trip_duration_per_day(df) -> DataFrame:
    # Виділяємо дату
    df = df.withColumn("start_date", to_date(col("start_time"), "MM/dd/yyyy HH:mm"))

    # групуємо за start_date, агрегую(agg) визначаючи середнє тривалості поїздок, результат в нову колонку average_trip_duration
    result = df.groupBy("start_date").agg(avg("tripduration").alias("average_trip_duration"))

    # замість Hadoop, вирішив конвертувати в Pandas і вже за допомогою нього зберігати в csv
    result.toPandas().to_csv("/opt/bitnami/spark/jobs/out/avg_trip_duration_per_day.csv", index=False)
    return result


# кількість поїздок на день
def trips_per_day(df) -> DataFrame:
    # Виділяємо дату
    df = df.withColumn("start_date", to_date(col("start_time"), "MM/dd/yyyy HH:mm"))

    # групуємо за start_date, агрегуємо(agg) визначаючи кількість поїздок на день, результат в нову колонку trip_count
    result = df.groupBy("start_date").agg(count("trip_id").alias("trip_count"))

    # збереження в csv
    result.toPandas().to_csv("/opt/bitnami/spark/jobs/out/trips_per_day.csv", index=False)
    return result


# найпопулярніша початкова станція для кожного місяця
def most_popular_start_station_by_month(df):
    # Виділяємо рік і місяць
    df = df.withColumn("month", month(to_date(col("start_time"), "MM/dd/yyyy HH:mm")))
    df = df.withColumn("year", year(to_date(col("start_time"), "MM/dd/yyyy HH:mm")))

    # Рахуємо кількість поїздок з кожної станції в кожному місяці
    grouped = df.groupBy("year", "month", "from_station_name").agg(count("*").alias("trip_count"))
    
    # Вікно для ранжування поїздок по кожному місяцю
    window_spec = Window.partitionBy("year", "month").orderBy(col("trip_count").desc())

    # Додаємо ранг і відбираємо лише топ-1 для кожного місяця
    result = grouped.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1).drop("rank")
    
    # збереження в csv
    result.toPandas().to_csv("/opt/bitnami/spark/jobs/out/popular_stations_by_month.csv", index=False)

    return result


def top_3_stations_for_day_since_two_week(df) -> DataFrame:
    # Виділяємо дату
    df = df.withColumn("start_date", to_date(col("start_time"), "MM/dd/yyyy HH:mm"))

    # визначаємо останню дату з датасету
    latest_date = df.select(col("start_date")).distinct().orderBy(desc("start_date")).limit(1).collect()[0][0]

    # визначаємо дату, яка на 14 днів раніше від latest_date
    last_two_weeks = df.filter(col("start_date") >= latest_date - expr("INTERVAL 14 DAYS")) 
    
    # групуємо по start_date, from_station_name, обчислюємо кількість поїздок (trip_id) для кожної станції за кожен день
    top_3_stations = last_two_weeks.groupBy("start_date", "from_station_name").agg(count("trip_id").alias("count"))

     # використовуємо той самий прийом з вікном і розділом на групи по даті, потім ті групи сортуємо за спаданням по кількості поїздок count
    top_3_stations = top_3_stations.withColumn("popularity_top", row_number().over(Window.partitionBy("start_date").orderBy(desc("count"))))

    # перші три по популярності
    result = top_3_stations.filter(col("popularity_top") <= 3)

    # збереження в csv
    result.toPandas().to_csv("/opt/bitnami/spark/jobs/out/top_3_stations_for_day_since_two_week.csv", index=False)
    return result

# середня тривалість поїздок за статтю
def avg_duration_by_gender(df) -> DataFrame:
    # групуємо за статтю і визначаємо середнє поїздок
    result = df.groupBy("gender").agg(avg("tripduration").alias("average_duration"))
    result.toPandas().to_csv("/opt/bitnami/spark/jobs/out/avg_duration_by_gender.csv", index=False)
    return result

def main():
    spark = SparkSession.builder.appName("Journey Info").getOrCreate()
    df = spark.read.csv("/opt/bitnami/spark/jobs/Divvy_Trips_2019_Q4.csv", header=True, inferSchema=True)
    os.makedirs("/opt/bitnami/spark/jobs/out", exist_ok=True)

    result_avg_trip_duration_per_day = avg_trip_duration_per_day(df)
    result_avg_trip_duration_per_day.show()

    result_trips_per_day = trips_per_day(df)
    result_trips_per_day.show()

    result_most_popular_start_station_by_month = most_popular_start_station_by_month(df)
    result_most_popular_start_station_by_month.show()

    result_top_3_stations_for_day_since_two_week = top_3_stations_for_day_since_two_week(df)
    result_top_3_stations_for_day_since_two_week.show()

    result_avg_duration_by_gender = avg_duration_by_gender(df)
    result_avg_duration_by_gender.show()

    spark.stop()

if __name__ == "__main__":
    main()