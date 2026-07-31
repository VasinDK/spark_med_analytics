# 🏥 sparkmedanalytics

Высокотехнологичный дата-инженерный пайплайн корпоративного уровня для пакетной (Batch) обработки сырых медицинских данных (истории визитов, хронические заболевания, показатели пациентов). 

Проект реализует концепцию **Data Lakehouse** в облаке **Yandex Cloud** с использованием **Apache Spark** и **S3 Object Storage**, транзакционного формата таблиц **Apache Iceberg**, оркестрации в **Apache Airflow**, инкрементальной сборки витрин с помощью **dbt** для **ClickHouse** и визуализации в **Apache Superset**.

---

## 🏗 Архитектура данных & Пайплайн (Medallion Architecture)

```text
               ┌─────────────────────────────────────────┐
               │  Сырые медицинские данные (Raw JSON)   │
               └────────────────────┬────────────────────┘
                                    │
                                    ▼ [PySpark ETL: Silver Job]
               ┌────────────────────┴────────────────────┐
               │ Наложение схемы, валидация & сбор DQ    │
               └────────┬────────────────────────┬───────┘
                        │                        │
          [Массив errors ПУСТОЙ]          [Массив errors НЕ пустой]
                        │                        │
                        ▼                        ▼
 ┌──────────────────────┴──────┐          ┌──────┴──────────────────────┐
 │ Yandex S3: Silver (Iceberg) │          │  Yandex S3: Quarantine/DLQ  │
 └──────────────┬──────────────┘          └─────────────────────────────┘
                │
                ▼ [PySpark Aggregation]
 ┌─────────────────────────────┐
 │  Yandex S3: Gold (Iceberg)  │
 └──────────────┬──────────────┘
                │
                ▼ [Airflow: dbt-clickhouse container]
 ┌─────────────────────────────┐
 │       DWH: ClickHouse       │
 └──────────────┬──────────────┘
                │
                ▼ [BI Analytics]
 ┌─────────────────────────────┐
 │       Apache Superset       │
 └─────────────────────────────┘
```

1. **Конфигурации и скрипты:** Файлы конфигурации, схемы данных и скрипты автоматизации хранятся в облачном S3-хранилище и динамически считываются компонентами.
2. **Bronze Layer:** Исходные JSON-файлы медицинских визитов, поступающие в Yandex Object Storage (S3).
3. **Bronze ➡️ Silver:** `PySpark`-джоб выполняет кастомную фильтрацию и валидацию данных, отправляя брак в изолированный S3-карантин (с контролем порога `CriticalDataQualityError`), а валидные данные обогащает (ID, BMI, типы дат) и сохраняет в таблицы Silver-слоя.
4. **Silver ➡️ Gold:** `PySpark` производит агрегацию данных из Silver-слоя Iceberg, формируя готовые бизнес-метрики в **Gold Layer (Iceberg)**.
5. **Gold ➡️ ClickHouse (dbt):** Airflow запускает **dbt**, который инкрементально считывает дельту из Gold Iceberg и обновляет аналитические таблицы в **ClickHouse**.
6. **BI-Слой:** Подключенный к ClickHouse **Apache Superset** визуализирует медицинские дашборды и графики.

---

## 🛠 Технологический стек

* **Облачная инфраструктура:** Yandex Cloud (Вычислительный кластер **Data Proc** для тяжелых Spark-задач, Virtual Machines, Object Storage S3).
* **Контейнеризация:** Docker & Docker-compose (ClickHouse, dbt, Apache Airflow, Apache Superset).
* **Оркестрация:** Apache Airflow (запуск ежедневно в `02:00` UTC).
* **Мониторинг:** Интегрированный **Telegram Alert Bot** для мгновенного оповещения о сбоях на любом этапе.
* **Табличный формат:** Apache Iceberg (Hadoop Catalog поверх S3).
* **Вычислительный движок:** Apache Spark (PySpark) со строгим контролем кэширования (`persist(StorageLevel.MEMORY_AND_DISK)` / `unpersist()`).
* **Преобразование данных:** dbt (data build tool) с адаптером под ClickHouse.
* **Аналитическое DWH:** ClickHouse (высокопроизводительная OLAP СУБД).
* **Качество кода:** Автоматическое тестирование Python-кода с помощью библиотеки **pytest**.

---

## 📁 Структура проекта

```text
├── airflow/
│   ├── dags/
│   │   └── medical_etl_dag.py  # Сценарий Airflow с Telegram-алертингом
│   └── plugins/
│       └── alerts.py           # Логика отправки нотификаций в Telegram
├── dbt_project/                # Модели dbt для转换данных в ClickHouse
│   ├── models/
│   └── dbt_project.yml
├── docker/
│   └── docker-compose.yml     # Стек: ClickHouse, Apache Superset, Apache Airflow
├── src/
│   ├── core/
│   │   ├── session.py          # Инициализация Spark-сессии
│   │   ├── data_catalog_registry.py
│   │   └── writer.py           # Логика сохранения данных
│   ├── spark_jobs/
│   │   ├── bronze_to_silver.py # Заполнение Silver-слоя, валидация и очистка
│   │   └── silver_to_gold.py   # Сборка Iceberg Gold таблиц
│   ├── utils/
│   │   ├── metrics_validate.py # Класс сбора DQ-метрик (valid_rows, error_percent)
│   │   └── action_context.py   # Менеджер контекста выполнения Spark-шагов
│   ├── decorators.py           # Декоратор @monitor_job для профилирования
│   ├── exceptions.py           # Кастомные ошибки (CriticalDataQualityError)
│   └── constants.py            # Тексты логов и кодов ошибок
├── tests/                      # Автоматические тесты Python (pytest)
│   ├── conftest.py             # Фикстуры для локальной Spark-сессии
│   └── test_transforms.py      # Модульные тесты функций трансформации и DQ
├── README.md
└── requirements.txt
```

---

## 🚀 Порядок развертывания и запуска

### 1. Переменные окружения и Конфигурация в S3
* Загрузите файлы конфигурации инфраструктуры, схемы данных и скрипты автоматизации в бакет Yandex Object Storage.
* Создайте файл `.env` в корневом каталоге на управляющей ВМ в Yandex Cloud:
```env
S3_ACCESS_KEY=ваш_ключ_yandex_cloud
S3_SECRET_KEY=ваш_секретный_ключ
TELEGRAM_BOT_TOKEN=token_for_alerts
TELEGRAM_CHAT_ID=chat_id_for_alerts
CLICKHOUSE_PASSWORD=secure_password
```

### 2. Запуск инфраструктуры (DWH + BI + Orchestration)
Разверните все необходимые сервисы локально на ВМ или в облачном окружении:
```bash
docker-compose -f docker/docker-compose.yml up -d
```
*После запуска Apache Superset будет доступен по адресу `http://localhost:8088` для работы с дашбордами ClickHouse.*

### 3. Запуск автоматических тестов
Перед деплоем пайплайна на кластер Yandex Data Proc запустите проверку Python-логики:
```bash
pytest tests/
```

### 4. Принцип работы расписания в Airflow
Каждые сутки в **02:00** запускается DAG:
1. Выделяется сессия на кластере **Yandex Data Proc** для обработки тяжелых JSON-файлов через Spark.
2. Происходит поэтапное заполнение слоев Silver и Gold в формате Iceberg.
3. В случае любого необработанного исключения или падения из-за превышения порога брака данных (`CriticalDataQualityError`), дежурный инженер моментально получает нотификацию в Telegram.
