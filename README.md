# Product-Category PySpark

## 📄 Описание

Проект демонстрирует, как с помощью PySpark объединить продукты и категории в одну таблицу с парами:

- Имя продукта
- Имя категории

Для продуктов без категорий в результат подставляется значение `"No Category"`.

---
## ⚙️ Установка

1. Установить зависимости:

```bash
pip install -r requirements.txt
```

## ⚠️ Убедиться, что на компьютере установлены:

* Java (JDK 8+)
* PySpark
* Hadoop Winutils (опцианально)

##  🛠️ Как работает код
Создаются тестовые датафреймы продуктов, категорий и их связей.

Выполняется левое соединение (left join) с подстановкой "No Category" для продуктов без категорий.

Результат выводится через .show() в консоль.
