# ====================================================================================
# MAKEFILE ДЛЯ SPARK MEDICAL ANALYTICS ETL
# ====================================================================================

.PHONY: sync generate test lint build clean run-local run-dev refs-dev silver-dev gold-dev help

-include .env
export

help:
	@echo "Доступные команды:"
	@echo "  make sync         - Создать виртуальное окружение и установить зависимости"
	@echo "  make generate     - Сгенерировать тестовые данные (без справочников) в папку data/"
	@echo "  make test         - Запустить локальные юнит-тесты через pytest"
	@echo "  make lint         - Проверить форматирование кода линтером black"
	@echo "  make refs-dev     - Обновление справочников и отправка статических файлов в облако"
	@echo "  make silver-dev   - Запуск скриптов для подготовки слоя silver"
	@echo "  make gold-dev     - Запуск скриптов для подготовки слоя gold"
# 	@echo "  make run-local    - Запустить ETL-джобу локально на тестовых данных"
	@echo "  make run-dev      - Запустить ETL-джобу на dev s3"
	@echo "  make build        - Собрать .whl пакет для деплоя на Yandex Data Proc"
	@echo "  make clean        - Удалить временные файлы, кэш и сборки"

# Инициализация проекта и синхронизация зависимостей
sync:
	uv sync

# Генерация тестовых данных (визиты, без справочников) в локальную папку data/
generate:
	mkdir -p data
	uv run python scripts/generate_data.py

# Обновление справочников и отправка статических файлов в облако
refs-dev:
	yc storage s3 cp data/10_patient_visits_1m.json s3://spark-medanalytics-dev-bronze/patient_visits_1m.json &&
	yc storage s3 cp data/departments.csv s3://spark-medanalytics-dev-bronze/departments.csv &&
	yc storage s3 cp data/professions.csv s3://spark-medanalytics-dev-bronze/professions.csv &&
	yc storage s3 cp jobs/load_references.py s3://$(CODE_BUCKET)/load_references.py &&
	yc dataproc job create-pyspark \
		--cluster-name $(CLUSTER) \
		--name IcebergJob \
		--main-python-file-uri s3a://$(CODE_BUCKET)/load_references.py \
		--properties spark.hadoop.fs.s3a.endpoint=storage.yandexcloud.net,spark.pyspark.python=/usr/bin/python3,spark.pyspark.driver.python=/usr/bin/python3 \
		--repositories https://maven.org \
		--packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3,org.apache.iceberg:iceberg-aws-bundle:1.4.3 \
		--async=false

# Пробный запуск ETL джобы на dev s3
silver-dev: test lint build
	export SPARK_ENV=dev && 
	yc storage s3 cp jobs/bronze_to_silver.py s3://$(CODE_BUCKET)/bronze_to_silver.py &&
	yc storage s3 cp dist/$(WHL_FILE) s3://$(CODE_BUCKET)/$(WHL_FILE) &&
	yc dataproc job create-pyspark \
		--cluster-name $(CLUSTER) \
		--name IcebergJob \
		--main-python-file-uri s3a://$(CODE_BUCKET)/bronze_to_silver.py \
		--python-file-uris s3a://$(CODE_BUCKET)/$(WHL_FILE) \
		--properties spark.hadoop.fs.s3a.endpoint=storage.yandexcloud.net,spark.pyspark.python=/usr/bin/python3,spark.pyspark.driver.python=/usr/bin/python3 \
		--repositories https://maven.org \
		--packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3,org.apache.iceberg:iceberg-aws-bundle:1.4.3 \
		--async=false
	
gold-dev:
	echo $(CLUSTER)

# Запуск локальных юнит-тестов на pytest
test:
	uv run pytest tests/

# Проверка форматирования кода
lint:
	uv run black src/ jobs/ scripts/ tests/ --check

# Локальный пробный запуск ETL джобы на вашем компьютере (для проверки логики)
# run-local:
# 	export SPARK_ENV=dev && uv run python jobs/bronze_to_silver.py

# Запуск ETL джобы на prod сервере
# run-prod: test lint build

# Сборка стабильного .whl пакета для отправки на кластер Data Proc
build: clean
	uv build --wheel

# Очистка репозитория от временного мусора, кэша тестов и логов
clean:
	rm -rf dist/ .pytest_cache/ .uv/ uv.lock spark-warehouse/ metastore_db/ derby.log
	find . -name "__pycache__" -exec rm -rf {} +
	find . -name "*.pyc" -exec rm -f {} +
