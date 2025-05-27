# pr_08
# Практическая работа по бизнес-аналитике

## Цель:
Определить наиболее эффективный метод загрузки данных (малых и больших объемов) из CSV-файлов в СУБД PostgreSQL, сравнивая время выполнения для методов: pandas.to_sql(), psycopg2.copy_expert() (с файлом и с io.StringIO), и пакетная вставка (psycopg2.extras.execute_values).

## Задачи:
1. Подключиться к предоставленной базе данных PostgreSQL.
2. Проанализировать структуру исходных CSV-файлов (upload_test_data.csv ,  upload_test_data_big.csv).
3. Создать эскизы ER-диаграмм для таблиц, соответствующих структуре CSV-файлов.
4. Реализовать три различных метода загрузки данных в PostgreSQL(pandas.to_sql(), copy_expert(), io.StringIO).
5. Измерить время, затраченное каждым методом на загрузку данных из малого файла (upload_test_data.csv).
6. Измерить время, затраченное каждым методом на загрузку данных из большого файла (upload_test_data_big.csv).
7. Визуализировать результаты сравнения времени загрузки с помощью гистограммы (matplotlib).
8. Сделать выводы об эффективности каждого метода для разных объемов данных.

## Выполнение задания
Перед началом работы выполним подключение к билбиотекам:
```python
!pip install psycopg2-binary pandas sqlalchemy matplotlib numpy
```
Затем подключаем несколько библиотек для работы с базами данных, анализа данных и визуализации:
```python
import psycopg2
from psycopg2 import Error
from psycopg2 import extras # For execute_values
import pandas as pd
from sqlalchemy import create_engine
import io # For StringIO
import time
import matplotlib.pyplot as plt
import numpy as np
import os # To check file existence
```
 Укажем путь к файлам csv и подключимся к БД:
 ```python
small_csv_path = r'"C:\Users\626\Downloads\upload_test_data.csv"'
```

 ```python
big_csv_path = r'"C:\Users\626\Downloads\upload_test_data_big.csv"'
```

 ```python
!ls
```
```python
# @markdown Установка и импорт необходимых библиотек.


print("Libraries installed and imported successfully.")

# Database connection details (replace with your actual credentials if different)
DB_USER = "postgres"
DB_PASSWORD = "1"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "lect_08_bda_big_data"

# CSV File Paths (Ensure these files are uploaded to your Colab environment)
small_csv_path = 'upload_test_data.csv'
big_csv_path = 'upload_test_data_big.csv' # Corrected filename

# Table name in PostgreSQL
table_name = 'sales_data'
```
## Вариант 8
## Задание 1. Создать таблицы sales_small, sales_big.
```python
# @title # 3. Database Connection Test
# @markdown Проверка соединения с базой данных PostgreSQL.

connection = None
cursor = None
engine = None # For pandas.to_sql

try:
    # Establish connection using psycopg2
    print("Connecting to PostgreSQL database using psycopg2...")
    connection = psycopg2.connect(user=DB_USER,
                                  password=DB_PASSWORD,
                                  host=DB_HOST,
                                  port=DB_PORT,
                                  database=DB_NAME)
    connection.autocommit = False # Important for COPY and batch inserts within transactions
    cursor = connection.cursor()

    print("PostgreSQL server information:")
    print(connection.get_dsn_parameters(), "\n")
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print(f"Successfully connected to: {record[0]}\n")

    # Create SQLAlchemy engine for pandas
    print("Creating SQLAlchemy engine...")
    engine_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(engine_url)
    print("SQLAlchemy engine created successfully.")


except (Exception, Error) as error:
    print(f"Error while connecting to PostgreSQL: {error}")
    # Ensure resources are closed even if connection fails partially
    if cursor:
        cursor.close()
    if connection:
        connection.close()
    if engine:
        engine.dispose() # Close SQLAlchemy engine pool
    connection, cursor, engine = None, None, None # Reset variables

# We keep the connection open for the rest of the script.
# It will be closed in the final step.
```
Перед началом работы убедимся, что базовые коды выполняются
```python
# @title Вспомогательные Функции (Часть Базового Кода)
# Убедитесь, что эта ячейка выполняется ПЕРЕД запуском кода варианта

# ... (здесь должны быть определения connect_db, close_db, execute_sql, load_df_from_sql и т.д.)

def execute_sql(sql_query, fetch=False):
    """Выполняет SQL-запрос и опционально возвращает результаты."""
    if not connection or not cursor:
        print("Нет подключения к БД.")
        return None
    try:
        # print(f"Выполнение SQL: {sql_query[:100]}...") # Отладка: показать начало запроса
        cursor.execute(sql_query)
        if fetch:
            results = cursor.fetchall()
            # print("Результаты получены.") # Отладка
            return results
        else:
            # Для запросов не SELECT (CREATE, DROP, INSERT, UPDATE, DELETE)
            # connection.commit() # Не требуется при autocommit=True
            # print("Запрос успешно выполнен (без fetch).") # Отладка
            return True # Возвращаем True для индикации успеха выполнения не-SELECT запроса
    except (Exception, Error) as error:
        print(f"Ошибка выполнения SQL: {error}")
        # connection.rollback() # Не требуется при autocommit=True
        return None # Возвращаем None для индикации ошибки

# --- Функция создания таблицы ---
def create_table(tbl_name):
    """Создает стандартную таблицу для данных продаж, удаляя ее, если она существует."""
    print(f"\nПопытка создать таблицу: {tbl_name}")
    # Шаг 1: Удалить таблицу, если она уже существует
    # Это важно для повторяемости: гарантирует, что мы начинаем с чистого листа
    # и не получим ошибку "table already exists".
    drop_success = execute_sql(f"DROP TABLE IF EXISTS {tbl_name};")
    if drop_success is None: # Проверяем, не было ли ошибки при DROP
        print(f"Не удалось выполнить DROP TABLE для {tbl_name}. Создание таблицы отменено.")
        return # Прерываем создание, если DROP не удался

    # Шаг 2: Определить SQL-запрос для создания таблицы
    # Структура таблицы (колонки и типы данных) соответствует анализу
    # из исходной практической работы. Имя таблицы берется из аргумента tbl_name.
    create_query = f"""
    CREATE TABLE {tbl_name} (
        id INTEGER PRIMARY KEY,         -- Уникальный идентификатор, первичный ключ
        quantity INTEGER,               -- Количество
        cost NUMERIC(10, 2),            -- Стоимость (NUMERIC для точности)
        total_revenue NUMERIC(12, 2)    -- Общая выручка (NUMERIC для точности)
    );
    """
    print(f"Запрос на создание таблицы {tbl_name}:\n{create_query}")

    # Шаг 3: Выполнить SQL-запрос на создание таблицы
    create_success = execute_sql(create_query)
    if create_success:
        print(f"Таблица '{tbl_name}' успешно создана.")
    else:
        print(f"Не удалось создать таблицу '{tbl_name}'.")

# ... (здесь определения load_via_pandas, load_via_copy_file, load_via_copy_stringio и т.д.)


# @title Вспомогательные Функции (Часть Базового Кода - УБЕДИТЕСЬ, ЧТО ЭТА ЯЧЕЙКА ВЫПОЛНЕНА!)

# ... (импорты, константы, connect_db, close_db, execute_sql, load_df_from_sql, create_table) ...

# --- Функции Загрузки Данных ---

def load_via_copy_file(file_path, tbl_name):
    """Загружает данные из DataFrame через file, используя copy_expert."""
    # Проверка подключения и наличия данных
    if not connection or not cursor:
        print("Нет подключения к БД или DataFrame пуст.")
        return False # Возвращаем False при неудаче
    print(f"Загрузка данных в '{tbl_name}' с использованием copy_expert (file)...")
    start_time = time.time()
    buffer = io.StringIO() # Создаем буфер в памяти

    # Записываем DataFrame в буфер как CSV, включая заголовок
    try:
        df.to_csv(buffer, index=False, header=True, sep=',')
    except Exception as e:
         print(f"Ошибка конвертации DataFrame в CSV: {e}")
         buffer.close() # Закрываем буфер при ошибке
         return False # Возвращаем False при неудаче
    buffer.seek(0) # Перематываем буфер в начало для чтения
    # Формируем SQL команду COPY
    sql_copy = f"COPY {tbl_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')"
    try:
        # Выполняем COPY, передавая буфер как файл
        cursor.copy_expert(sql=sql_copy, file=buffer)
        # connection.commit() # Не требуется при autocommit=True
        duration = time.time() - start_time
        print(f"Успешно: Загрузка (file) в '{tbl_name}' завершена за {duration:.2f} сек.")
        return True # Возвращаем True при успехе
    except (Exception, Error) as error:
        print(f"ОШИБКА при выполнении copy_expert (file) для '{tbl_name}': {error}")
        # connection.rollback() # Не требуется при autocommit=True
        return False # Возвращаем False при неудаче
    finally:
        buffer.close() # Гарантируем закрытие буфера в любом случае

```


```python
def load_via_copy_file(file_path, tbl_name):
    """Загружает данные напрямую из CSV файла, используя copy_expert."""
    # Проверка подключения
    if not connection or not cursor:
       print("Нет подключения к БД.")
       return False # Возвращаем False при неудаче
    # Проверка существования файла
    if not os.path.exists(file_path):
        print(f"ОШИБКА: Файл '{file_path}' не найден.")
        return False # Возвращаем False при неудаче
    print(f"Загрузка данных в '{tbl_name}' с использованием copy_expert (file: {os.path.basename(file_path)})...")
    start_time = time.time()
    # Формируем SQL команду COPY
    sql_copy = f"COPY {tbl_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')"
    try:
        # Открываем файл для чтения и передаем его дескриптор в copy_expert
        with open(file_path, 'r', encoding='utf-8') as f:
            cursor.copy_expert(sql=sql_copy, file=f)
        # connection.commit() # Не требуется при autocommit=True
        duration = time.time() - start_time
        print(f"Успешно: Загрузка (file) в '{tbl_name}' завершена за {duration:.2f} сек.")
        return True # Возвращаем True при успехе
    except (Exception, Error) as error:
        print(f"ОШИБКА при выполнении copy_expert (file) для '{tbl_name}': {error}")
        # connection.rollback() # Не требуется при autocommit=True
        return False # Возвращаем False при неудаче

# ... (другие вспомогательные функции, если есть) ...

print("Вспомогательные функции определены.")
```

Получим результат:


![Screenshot_67](https://github.com/user-attachments/assets/3541af87-c300-4673-9b7b-7bef92151473)



```python
# @title Вспомогательные Функции (Часть Базового Кода - УБЕДИТЕСЬ, ЧТО ЭТА ЯЧЕЙКА ВЫПОЛНЕНА!)

# ... (импорты, константы, connect_db, close_db, execute_sql, create_table, ...) ...

# --- Функция загрузки данных из SQL в DataFrame ---
def load_df_from_sql(sql_query):
    """Выполняет SQL-запрос и загружает результаты в Pandas DataFrame."""
    # Проверяем наличие активного подключения
    if not connection: # Для read_sql_query достаточно объекта connection
        print("Нет подключения к БД для загрузки DataFrame.")
        return None # Возвращаем None при ошибке
    print(f"Загрузка данных из SQL в DataFrame: {sql_query[:100]}...") # Показываем начало запроса
    try:
        # Используем pandas.read_sql_query для выполнения запроса и создания DataFrame
        # Эта функция сама управляет курсором и соединением для чтения
        df = pd.read_sql_query(sql_query, connection)
        print(f"Успешно: Загружено {len(df)} строк в DataFrame.")
        return df # Возвращаем созданный DataFrame
    except (Exception, Error) as error:
        # Обрабатываем ошибки при выполнении запроса или создании DataFrame
        print(f"ОШИБКА при загрузке DataFrame из SQL: {error}")
        return None # Возвращаем None при ошибке

# ... (определения load_via_copy_stringio, load_via_copy_file и другие функции) ...

print("Вспомогательные функции определены.")
```

Получим результат:


![Screenshot_67](https://github.com/user-attachments/assets/e9a862c6-3e1d-4c33-8b5a-150783d28306)



Приступаем к созданию таблиц
```python
# -*- coding: utf-8 -*-
"""
Мини-проект: Решение для Варианта 8
"""

# @title Настройка окружения и подключение к БД (Запустить первым - Базовый код)
# ... (здесь должен быть весь ваш базовый код с импортами, константами, функциями)
# Убедитесь, что функции connect_db, create_table, execute_sql, load_df_from_sql,
# load_via_copy_stringio, load_via_copy_file определены и выполнены.
# ...

# --- Константы для этого варианта ---
small_table_name = 'sales_small'
big_table_name = 'sales_big'
small_csv_path = r"C:\Users\626\Downloads\upload_test_data.csv"
big_csv_path = r"C:\Users\626\Downloads\upload_test_data_big.csv"
# ----------------------------------


# Проверяем статус подключения перед продолжением
if not connection or not cursor:
    print("Подключение к базе данных неактивно. Пожалуйста, выполните настройку подключения.")
else:
    print("--- Запуск Варианта 8 (Упрощенная загрузка) ---")

    # --- Задача 1: Настройка таблиц ---
    print("\n--- Задача 1: Создание таблиц ---")
    create_table(small_table_name)
    create_table(big_table_name)
    # Функция create_table должна вывести сообщения об успехе/ошибке
```
Получаем результаты:


![Screenshot_68](https://github.com/user-attachments/assets/a4e0f34e-bfab-451e-991f-133b5d1cef45)



![Screenshot_69](https://github.com/user-attachments/assets/7f7de513-ec1d-4bba-95eb-7b2ea0ddc2f6)



## Задание 2. Загрузить малые данные методом copy_expert (file)
```python
        # --- Задача 2: Загрузка малых данных (copy_expert file) ---
    print(f"\n--- Задача 2: Загрузка данных из '{small_csv_path}' в '{small_table_name}' (метод file) ---")
    if os.path.exists(small_csv_path):
        load_via_copy_file(small_csv_path, small_table_name)

    else:
        # Сообщение, если файл не найден
        print(f"ОШИБКА: Файл '{small_csv_path}' не найден. Загрузка не выполнена.")
```
Получим результат:


![Screenshot_70](https://github.com/user-attachments/assets/da4f794b-0754-44fc-998b-04e1517afeb6)




## Задание 3. Загрузить большие данные методом copy_expert (file)
```python
            # --- Задача 3: Загрузка больших данных (copy_expert file) ---
    print(f"\n--- Задача 3: Загрузка данных из '{big_csv_path}' в '{big_table_name}' (метод file) ---")
    if os.path.exists(big_csv_path):
        # Напрямую вызываем функцию загрузки из файла
        load_via_copy_file(big_csv_path, big_table_name)
        # Функция load_via_copy_file выведет сообщение об успехе/ошибке загрузки
    else:
        # Сообщение, если файл не найден
        print(f"ОШИБКА: Файл '{big_csv_path}' не найден. Загрузка не выполнена.")
```

Получим результат:


![Screenshot_71](https://github.com/user-attachments/assets/3f5069bb-d87b-4504-a5a1-2eb0056f0839)



## Задание 4. SQL Анализ: подсчитать записи в sales_small, где total_revenue > 200.
```python
            # --- Задача 4: SQL Анализ ---
    print("\n--- Задача 4: SQL Анализ таблицы sales_small ---")
    sql_query_task4 = f"""
    SELECT COUNT(*)
    FROM {small_table_name}
    WHERE total_revenue > 200;
    
    """
    print("Выполнение SQL запроса:")
    print(sql_query_task4)
    results_task4 = execute_sql(sql_query_task4, fetch=True)

    if results_task4 is not None:
        print("\nРезультаты запроса (COUNT(*)):")
        if results_task4:
            for row in results_task4:
                print(row)
        else:
            print("Запрос успешно выполнен, но не вернул строк.")
    else:
        print("Ошибка выполнения SQL запроса.")
```

Получим результат:


![Screenshot_72](https://github.com/user-attachments/assets/9a54cb90-a641-4f31-af6b-b63ba74ba636)



## Задание 5. Python/Colab Анализ и Визуализация: построить гистограмму quantity из sales_big (LIMIT 50000).
```python
        # --- Задача 5: Python Визуализация ---
print("\n--- Задача 5: Визуализация quantity из sales_big ---")
sql_query_task5 = f"SELECT quantity FROM {big_table_name} LIMIT 50000;"
print(f"Получение данных для графика: {sql_query_task5}")

df_plot_data = load_df_from_sql(sql_query_task5)

if df_plot_data is not None and not df_plot_data.empty:
    print(f"Загружено {len(df_plot_data)} строк для построения графика.")
    
    fig, ax = plt.subplots(figsize=(10, 6))
    # Построение гистограммы вместо scatter plot
    ax.hist(df_plot_data['quantity'], bins=30, color='skyblue', edgecolor='black')
    
    ax.set_xlabel('Количество (Quantity)')
    ax.set_ylabel('Частота') 
    ax.set_title('Распределение Quantity (50,000 записей)') 
    ax.grid(True, linestyle='--', alpha=0.6)
    plt.show()
    
elif df_plot_data is not None and df_plot_data.empty:
    print("Запрос выполнен, но данные из sales_big для графика не получены.")
else:
    print("Не удалось загрузить данные из sales_big для построения графика.")

print("\n--- Вариант 8 Завершен ---")



    # закрываем соединение 
if cursor:
        cursor.close()
if connection:
        connection.close()
print("Соединение с базой данных закрыто")


# --- Важно: Закройте соединение в конце вашего ноутбука ---
# print("\n--- Очистка ресурсов ---")
# close_db()
```
Получаем результат


![Screenshot_73](https://github.com/user-attachments/assets/6a46c874-12fe-4b84-ab9d-6828ece911c7)



## Вывод
Научилась определять наиболее эффективный метод загрузки данных (малых и больших объемов) из CSV-файлов в СУБД PostgreSQL, сравнивая время выполнения для методов: pandas.to_sql(), psycopg2.copy_expert() (с файлом и с io.StringIO), и пакетная вставка (psycopg2.extras.execute_values).

## Структура репозитория:
- `ERD_diagram_sales_small.png` — ERD диаграмма таблицы sales_small.
- `ERD_diagram_sales_big.png` — ERD диаграмма таблицы sales_big.
- `Gubaidulina_Alina_Ilshatovna_pr8.ipynb` — Jupyter Notebook с выполнением всех заданий.
