# ====================================================================================
# MAKEFILE ДЛЯ SPARK MEDICAL ANALYTICS ETL

# Предворительно должны быть установлены:
# makefile, uv, yq
# ====================================================================================

-include .env
export $(shell sed 's/=.*//' .env)

SPARK_ENV ?= dev

CLICKHOUSE_USER ?= user
CLICKHOUSE_PASSWORD ?= 123
CLICKHOUSE_HOST ?= your-clickhouse-host
CLICKHOUSE_PORT ?= 8123

CLUSTER_NAME := $(shell uv run yq -r '.infrastructure.cluster_name' config/$(SPARK_ENV)_config.yaml)
CODE_BUCKET  := $(shell uv run yq -r '.infrastructure.code_bucket' config/$(SPARK_ENV)_config.yaml)
DEPENDENCIES  := $(shell uv run yq -r '.infrastructure.dependencies' config/$(SPARK_ENV)_config.yaml)
S3_DEPARTMENTS_CSV := $(shell uv run yq -r '.s3.departments_csv | .bucket + "/" + .path' config/$(SPARK_ENV)_config.yaml)
S3_PROFESSIONS_CSV := $(shell uv run yq -r '.s3.professions_csv | .bucket + "/" + .path' config/$(SPARK_ENV)_config.yaml)
WHL_FILE := $(shell uv run yq -r '.infrastructure.whl_file' config/$(SPARK_ENV)_config.yaml)
SCHEMAS_TABLES := $(shell uv run yq -r '.infrastructure.schemas' config/$(SPARK_ENV)_config.yaml)
REPOSITORIES := $(shell uv run yq -r '.repositories' config/$(SPARK_ENV)_config.yaml)
PACKAGES := $(shell uv run yq -r '.packages' config/$(SPARK_ENV)_config.yaml)
PACKAGES_GOLD := $(shell uv run yq -r '.packages_gold' config/$(SPARK_ENV)_config.yaml)
DB_SILVER_CAT := $(shell uv run yq -r '.databases.silver.catalog' config/$(SPARK_ENV)_config.yaml)
DB_GOLD_CLICK_CAT := $(shell uv run yq -r '.databases.gold_clickhouse.catalog' config/$(SPARK_ENV)_config.yaml)
SILVER := $(shell uv run yq -r '.s3.silver_warehouse | .bucket + "/" + .path' config/$(SPARK_ENV)_config.yaml)
STORAGE := $(shell uv run yq -r '.s3.storage' config/$(SPARK_ENV)_config.yaml)
LOG_LEVEL := $(shell uv run yq -r '.log_level.spark_sql' config/$(SPARK_ENV)_config.yaml)
S3_BRONZE_PATH := $(shell uv run yq -r '.s3.bronze' config/$(SPARK_ENV)_config.yaml)


.PHONY: sync generate test lint build clean help
		refs-dev silver-dev gold-dev 

help:
	@echo "Доступные команды:"
	@echo "  make sync         - Создать виртуальное окружение и установить зависимости"
	@echo "  make generate     - Сгенерировать тестовые данные (без справочников) в папку data/"
	@echo "  make test         - Запустить локальные юнит-тесты через pytest"
	@echo "  make lint         - Проверить форматирование кода линтером black"
	@echo "  make refs-dev     - Обновление справочников и отправка статических файлов в облако"
	@echo "  make silver-dev   - Запуск скриптов для подготовки слоя silver"
	@echo "  make gold-dev     - Запуск скриптов для подготовки слоя gold"
	@echo "  make build        - Собрать .whl пакет для деплоя на Yandex Data Proc"
	@echo "  make clean        - Удалить временные файлы, кэш и сборки"

# Инициализация проекта и синхронизация зависимостей
sync:
	uv sync

# Генерация тестовых данных (визиты, без справочников) в локальную папку data/
generate:
	mkdir -p data
	uv run python scripts/generate_data.py

# Запуск локальных юнит-тестов на pytest
test:
	uv run pytest tests/

# Проверка форматирования кода
lint:
# 	uv run black src/ jobs/ scripts/ tests/ --check
	uv run black jobs/bronze_to_silver.py

# Сборка стабильного .whl пакета для отправки на кластер Data Proc
build: 
	-uv build --wheel && \
	yc storage s3 cp dist/$(WHL_FILE) s3://$(CODE_BUCKET)/$(WHL_FILE)
# 	-yc storage s3 cp jobs/load_references.py s3://$(CODE_BUCKET)/load_references.py
	-yc storage s3 cp jobs/bronze_to_silver.py s3://$(CODE_BUCKET)/bronze_to_silver.py	
	-yc storage s3 cp config/${SPARK_ENV}_config.yaml s3://$(CODE_BUCKET)/$(SPARK_ENV)_config.yaml
	-yc storage s3 cp data/10_patient_visits_1m.json s3://$(S3_BRONZE_PATH)/10_patient_visits_1m.json
# 	-yc storage s3 cp data/departments.csv s3://$(S3_DEPARTMENTS_CSV)
# 	-yc storage s3 cp data/professions.csv s3://$(S3_PROFESSIONS_CSV)
	

# Очистка репозитория от временного мусора, кэша тестов и логов
clean:
	rm -rf src/sparkmedanalytics.egg-info/ dist/ .pytest_cache/ .uv/ uv.lock spark-warehouse/ metastore_db/ derby.log
	find . -name "__pycache__" -exec rm -rf {} +
	find . -name "*.pyc" -exec rm -f {} +

# Скачивание зависимостей
deps:
	uv pip compile pyproject.toml -o requirements.txt && \
	grep -E -v "pyspark|py4j|# " requirements.txt > requirements-cloud.txt && \
	rm -rf dist/dependencies dist/unpacked_libs && \
	mkdir -p dist/dependencies dist/unpacked_libs && \
	uv run pip download -r requirements-cloud.txt -d dist/dependencies/ && \
	for whl in dist/dependencies/*.whl; do unzip -q $$whl -d dist/unpacked_libs/; done && \
	(cd dist/unpacked_libs && zip -q -r ../dependencies.zip .) && \
	yc storage s3 cp dist/dependencies.zip s3://$(CODE_BUCKET)/dependencies.zip && \
	yc storage s3 cp config/schemas.yaml s3://$(CODE_BUCKET)/$(SCHEMAS_TABLES)

# Обновление схемы данных iceberg
migrate-iceberg:
	@echo "Синхронизация схем Iceberg с YAML конфигом..."
	$(SPARK_SUBMIT) scripts/sync_iceberg_schemas.py

# Обновление справочников и зависимостей
refs-dev: build
#  deps
	yc dataproc job create-pyspark \
		--cluster-name $(CLUSTER_NAME) \
		--name references \
		--main-python-file-uri s3a://$(CODE_BUCKET)/load_references.py \
		--python-file-uris s3a://$(CODE_BUCKET)/$(WHL_FILE),s3a://${CODE_BUCKET}/$(DEPENDENCIES) \
		--properties "spark.sql.catalog.$(DB_SILVER_CAT)=org.apache.iceberg.spark.SparkCatalog" \
		--properties "spark.sql.catalog.$(DB_SILVER_CAT).type=hadoop" \
		--properties "spark.sql.catalog.$(DB_SILVER_CAT).warehouse=s3a://$(SILVER)" \
		--properties "spark.sql.catalog.$(DB_SILVER_CAT).io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
		--properties "spark.sql.catalog.$(DB_SILVER_CAT).s3.endpoint=$(STORAGE)" \
		--properties "spark.sql.catalog.$(DB_SILVER_CAT).s3.path-style-access=true" \
		--properties "spark.sql.catalog.$(DB_SILVER_CAT).aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
		--properties "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
		--properties "spark.sql.logLevel=$(LOG_LEVEL)" \
		--repositories $(REPOSITORIES) \
		--packages $(PACKAGES) \
		--args "s3a://$(CODE_BUCKET)/$(SPARK_ENV)_config.yaml" \
		--async=false

silver-dev: build
	yc dataproc job create-pyspark \
		--cluster-name $(CLUSTER_NAME) \
		--name silver \
		--main-python-file-uri s3a://$(CODE_BUCKET)/bronze_to_silver.py \
		--python-file-uris s3a://$(CODE_BUCKET)/$(WHL_FILE),s3a://${CODE_BUCKET}/$(DEPENDENCIES) \
		--properties "spark.sql.catalog.$(DB_SILVER_CAT)=org.apache.iceberg.spark.SparkCatalog" \
		--properties "spark.sql.catalog.$(DB_SILVER_CAT).type=hadoop" \
		--properties "spark.sql.catalog.$(DB_SILVER_CAT).warehouse=s3a://$(SILVER)" \
		--properties "spark.sql.catalog.$(DB_SILVER_CAT).io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
		--properties "spark.sql.catalog.$(DB_SILVER_CAT).s3.endpoint=$(STORAGE)" \
		--properties "spark.sql.catalog.$(DB_SILVER_CAT).s3.path-style-access=true" \
		--properties "spark.sql.catalog.$(DB_SILVER_CAT).aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
		--properties "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
		--properties "spark.sql.logLevel=$(LOG_LEVEL)" \
		--repositories $(REPOSITORIES) \
		--packages $(PACKAGES) \
		--args "s3a://$(CODE_BUCKET)/$(SPARK_ENV)_config.yaml" \
		--async=false
	
gold-dev:build
	yc dataproc job create-pyspark \
		--cluster-name $(CLUSTER_NAME) \
		--name gold \
		--main-python-file-uri s3a://$(CODE_BUCKET)/silver_to_gold.py \
		--python-file-uris s3a://$(CODE_BUCKET)/$(WHL_FILE),s3a://${CODE_BUCKET}/$(DEPENDENCIES) \
		--properties "spark.sql.catalog.$(DB_SILVER_CAT)=org.apache.iceberg.spark.SparkCatalog" \
		--properties "spark.sql.catalog.$(DB_SILVER_CAT).type=hadoop" \
		--properties "spark.sql.catalog.$(DB_SILVER_CAT).warehouse=s3a://$(SILVER)" \
		--properties "spark.sql.catalog.$(DB_SILVER_CAT).io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
		--properties "spark.sql.catalog.$(DB_SILVER_CAT).s3.endpoint=$(STORAGE)" \
		--properties "spark.sql.catalog.$(DB_SILVER_CAT).s3.path-style-access=true" \
		--properties "spark.sql.catalog.$(DB_SILVER_CAT).aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
		--properties "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
		--properties "spark.sql.catalog.$(DB_GOLD_CLICK_CAT)=com.clickhouse.spark.ClickHouseCatalog" \
		--properties "spark.sql.catalog.$(DB_GOLD_CLICK_CAT).host=$(CLICKHOUSE_HOST)" \
		--properties "spark.sql.catalog.$(DB_GOLD_CLICK_CAT).http_port=$(CLICKHOUSE_PORT)" \
		--properties "spark.sql.catalog.$(DB_GOLD_CLICK_CAT).user=$(CLICKHOUSE_USER)" \
		--properties "spark.sql.catalog.$(DB_GOLD_CLICK_CAT).password=$(CLICKHOUSE_PASSWORD)" \
		--properties "spark.sql.logLevel=$(LOG_LEVEL)" \
		--repositories $(REPOSITORIES) \
		--packages $(PACKAGES),$(PACKAGES_GOLD) \
		--args "s3a://$(CODE_BUCKET)/$(SPARK_ENV)_config.yaml" \
		--async=false

check-env:
	@echo "Текущая среда: $(SPARK_ENV)"
	@echo "Ищем файл: config/$(SPARK_ENV)_config.yaml"
	@echo "Имя кластера из YAML: $(CLUSTER_NAME)"
	