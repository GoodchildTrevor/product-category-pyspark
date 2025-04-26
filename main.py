import os
# Прописываем пути к Пайтону и Хадупу явно на Windows
os.environ["HADOOP_HOME"] = r"C:\Program Files\hadoop"
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, lit


def get_product_category_pairs(df_products, df_relation, df_categories):
    """
    Объединяет датафреймы продуктов, связей и категорий, чтобы вернуть
    один датафрейм с двумя колонками: "product_name" и "category_name".
    Если у продукта нет категории, в "category_name" возвращается значение "Нет категории".

    Params:
    - df_products: DataFrame с колонками [product_id, product_name]
    - df_relation: DataFrame с колонками [product_id, category_id]
    - df_categories: DataFrame с колонками [category_id, category_name]

    Return:
    - DataFrame с колонками [product_name, category_name]
    """

    # Выполняем LEFT JOIN: сначала объединяем продукты и таблицу связей по product_id
    df_join = df_products.join(df_relation, on="product_id", how="left") \
        .join(df_categories, on="category_id", how="left")

    # Используем coalesce, чтобы для отсутствующих категорий подставить "Нет категории"
    df_result = df_join.select(
        "product_name",
        coalesce("category_name", lit("Нет категории")).alias("category_name")
    )
    return df_result


def create_test_dataframes(spark):
    """
    Создает тестовые датафреймы для примера.
    """
    # Продукты
    products_data = [
        (1, "Яблоко"),
        (2, "Огурец"),
        (3, "Клюква"),
        (4, "Киви")
    ]
    df_products = spark.createDataFrame(products_data, ["product_id", "product_name"])

    # Категории
    categories_data = [
        (10, "Овощи"),
        (20, "Фрукты"),
        (30, "Ягоды"),
    ]
    df_categories = spark.createDataFrame(categories_data, ["category_id", "category_name"])

    # Связи: несколько категорий для одного продукта или ни одной
    relation_data = [
        (1, 20),  # Яблоко -> Фрукты
        (2, 10),  # Огурец -> Овощи
        (3, 30),  # Клюква -> Ягоды
        # Киви не имеет записи, значит нет категории
    ]
    df_relation = spark.createDataFrame(relation_data, ["product_id", "category_id"])

    return df_products, df_relation, df_categories


if __name__ == "__main__":
    # Инициализация SparkSession
    spark = SparkSession.builder \
        .appName("ProductCategoryExample") \
        .master("local[*]") \
        .getOrCreate()

    # Создаем тестовые датафреймы
    df_products, df_relation, df_categories = create_test_dataframes(spark)

    # Получаем объединенный датафрейм с парами "Имя продукта – Имя категории"
    df_result = get_product_category_pairs(df_products, df_relation, df_categories)

    print("Результаты объединения:")
    df_result.show(truncate=False)

    # Остановка SparkSession
    spark.stop()
