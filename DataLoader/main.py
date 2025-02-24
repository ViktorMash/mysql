import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from tqdm import tqdm
from dotenv import load_dotenv
import sys
import os

# Загружаем переменные окружения
load_dotenv()

INITIAL_DATA = "../dataset/Air_Quality.csv"
CLEANED_DATA = "../dataset/clean_air_quality.csv"


def clean_air_quality_data():
    """
        Функция для очистки и преобразования данных о качестве воздуха
        :return: Очищенный и преобразованный pandas датафрейм, также он сохраняется в csv формате CLEANED_DATA
    """
    print("Начинаем очистку данных...")
    df = pd.read_csv(INITIAL_DATA, encoding='utf-8')

    # Переименовываем столбцы для SQL
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]

    # Удаляем пустые строки
    df = df.dropna(how='all')

    # Преобразуем типы данных
    # Целочисленные столбцы
    int_columns = ['unique_id', 'indicator_id']
    for col in int_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    # Обработка geo_join_id
    df['geo_join_id'] = pd.to_numeric(df['geo_join_id'], errors='coerce').astype('Int64')

    # Очистка текстовых полей
    text_columns = ['name', 'measure', 'measure_info', 'geo_type_name', 'geo_place_name']
    for col in text_columns:
        df[col] = df[col].astype(str).str.strip()
        # Заменяем пустые строки на NULL
        df[col] = df[col].replace({'nan': None, '': None})

    # Преобразование дат с учетом разных форматов
    def parse_date(date_str):
        formats = [
            '%m/%d/%Y',    # для формата 12/31/2022
            '%d.%m.%Y'     # для формата 31.12.2022
        ]

        for fmt in formats:
            try:
                return pd.to_datetime(date_str, format=fmt)
            except (ValueError, TypeError):
                continue
        return pd.NaT

    df['start_date'] = df['start_date'].apply(parse_date)

    # Очистка столбца data_value
    df['data_value'] = df['data_value'].str.replace(',', '').str.strip()
    df['data_value'] = pd.to_numeric(df['data_value'], errors='coerce')

    # Удаляем ненужный (пустой) столбец
    if 'unnamed:_10' in df.columns:
        df = df.drop('unnamed:_10', axis=1)

    # Удаляем дубликаты
    df = df.drop_duplicates()
    df.to_csv(CLEANED_DATA, index=False)
    print(f"Данные сохранены в {CLEANED_DATA}")

    return df


def import_data_to_mysql(cleaned_data_path, cfg):
    """
        Импортирует данные из CSV файла в MySQL

        Args:
            cleaned_data_path (str): Путь к очищенному CSV файлу
            cfg (dict): Конфигурация подключения к MySQL
    """
    table_name = cfg['table_name']
    try:
        # Создаем подключение к MySQL
        connection = mysql.connector.connect(
            host=cfg['host'],
            user=cfg['user'],
            password=cfg['password'],
            database=cfg['database'],
            charset=cfg['charset']
        )

        # Создаем engine для pandas
        engine_url = f"mysql+mysqlconnector://{cfg['user']}:{cfg['password']}@{cfg['host']}/{cfg['database']}"
        engine = create_engine(engine_url)

        # Читаем CSV файл
        print("Чтение CSV файла...")
        df = pd.read_csv(cleaned_data_path)

        # Загружаем данные чанками для эффективности
        chunk_size = 1000
        chunks = list(range(0, len(df), chunk_size))

        print(f"\nНачинаем загрузку данных в таблицу {table_name}")
        for i in tqdm(chunks, desc="Загрузка данных", unit="chunk"):
            chunk = df.iloc[i:i + chunk_size]
            chunk.to_sql(table_name,
                        engine,
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=chunk_size)

        print("\nДанные успешно загружены")

        # Проверяем количество загруженных строк
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {cfg['database']}.{table_name}")
        count = cursor.fetchone()[0]
        print(f"Всего загружено {count} строк")

    except Exception as e:
        print(f"Ошибка при импорте данных: {e}")
        sys.exit(1)
    finally:
        if 'connection' in locals():
            connection.close()


if __name__ == "__main__":

    mysql_config = {
        'host': 'localhost',
        'user': 'root',
        'password': os.getenv('MYSQL_PASSWORD'),  # Получаем пароль из переменных окружения
        'database': 'test_db',
        'charset': 'utf8mb4',
        'table_name': 'air_quality'
    }

    # Проверяем наличие пароля в .env
    if not mysql_config['password']:
        print("Ошибка: Не найден пароль MySQL в переменных окружения")
        sys.exit(1)

    # Очищаем данные
    clean_air_quality_data()

    # Импортируем в MySQL
    import_data_to_mysql(CLEANED_DATA, mysql_config)