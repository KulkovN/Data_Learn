from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as t
from fields import *


def extractor(spark: SparkSession, path: str) -> DataFrame:
    """Вернет дф, прочитанный по переданному пути. 
    Если путь не передан == прочтет файл дефолта """
    if not path:
        path = 'data/cars.csv'
    return spark.read.option('header', 'true').csv(path)


def outputer(df: DataFrame) -> DataFrame:
    """Обработает полученный дф Применям агрегирующие функции, 
    согласно задачам """
    return df.groupBy(manufacturer_name).agg(
        F.count(manufacturer_name).alias("Кол-во объявлений"),
        F.round(F.avg(year_produced)).cast(
            t.IntegerType()).alias("Средний год выпуска авто"),
        F.min(price_usd).alias("Минимальная цена"),
        F.max(price_usd).alias("Максимальная цена"),
    ).orderBy(F.col(manufacturer_name).desc())


def saver (df: DataFrame, name_fold ) -> None:
    """ Сохранит полученные данные после агрегации """
    df.coalesce(2).write.mode("overwrite").format(
        "json").save(name_fold)
    

def runer():
    """Стартует сессию """
    spark = SparkSession.builder.appName(
        "Практика").getOrCreate()
    if saver(outputer(extractor(spark,'')),
    "output.json.test"):
        spark.stop()


if __name__ == "__main__":
    runer()