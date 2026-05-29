# ====================================================================================
# MAKEFILE ДЛЯ SPARK MEDICAL ANALYTICS ETL
# ====================================================================================

-include .env
export

# SPARK_ENV_PROPS_SPACES = \
# spark.executorEnv.SPARK_ENV=$(SPARK_ENV),\
# spark.executorEnv.DB_CATALOG=$(DB_CATALOG),\
# spark.executorEnv.DB_SCHEMA=$(DB_SCHEMA),\
# spark.executorEnv.S3_STORAGE=$(S3_STORAGE),\
# spark.executorEnv.S3_BRONZE_PATH=$(S3_BRONZE_PATH),\
# spark.executorEnv.S3_DEPARTMENTS_CSV=$(S3_DEPARTMENTS_CSV),\
# spark.executorEnv.S3_PROFESSIONS_CSV=$(S3_PROFESSIONS_CSV),\
# spark.executorEnv.S3_SILVER_WAREHOUSE=$(S3_SILVER_WAREHOUSE),\
# spark.executorEnv.S3_QUARANTINE_PATH=$(S3_QUARANTINE_PATH),\
# spark.executorEnv.TABLE_NAME_VISITS=$(TABLE_NAME_VISITS),\
# spark.executorEnv.TABLE_NAME_DEPARTMENTS=$(TABLE_NAME_DEPARTMENTS),\
# spark.executorEnv.TABLE_NAME_PROFESSIONS=$(TABLE_NAME_PROFESSIONS),\
# spark.executorEnv.TABLE_NAME_SYMPTOMS=$(TABLE_NAME_SYMPTOMS),\
# spark.executorEnv.TABLE_NAME_CHRONIC=$(TABLE_NAME_CHRONIC),\
# spark.executorEnv.DQ_MIN_AGE=$(DQ_MIN_AGE),\
# spark.executorEnv.DQ_MAX_AGE=$(DQ_MAX_AGE),\
# spark.executorEnv.DQ_MIN_TEMP=$(DQ_MIN_TEMP),\
# spark.executorEnv.DQ_MAX_TEMP=$(DQ_MAX_TEMP),\
# spark.executorEnv.NAME_REF_JOB=$(NAME_REF_JOB),\
# spark.yarn.appMasterEnv.SPARK_ENV=$(SPARK_ENV),\
# spark.yarn.appMasterEnv.DB_CATALOG=$(DB_CATALOG),\
# spark.yarn.appMasterEnv.DB_SCHEMA=$(DB_SCHEMA),\
# spark.yarn.appMasterEnv.S3_STORAGE=$(S3_STORAGE),\
# spark.yarn.appMasterEnv.S3_BRONZE_PATH=$(S3_BRONZE_PATH),\
# spark.yarn.appMasterEnv.S3_DEPARTMENTS_CSV=$(S3_DEPARTMENTS_CSV),\
# spark.yarn.appMasterEnv.S3_PROFESSIONS_CSV=$(S3_PROFESSIONS_CSV),\
# spark.yarn.appMasterEnv.S3_SILVER_WAREHOUSE=$(S3_SILVER_WAREHOUSE),\
# spark.yarn.appMasterEnv.S3_QUARANTINE_PATH=$(S3_QUARANTINE_PATH),\
# spark.yarn.appMasterEnv.TABLE_NAME_VISITS=$(TABLE_NAME_VISITS),\
# spark.yarn.appMasterEnv.TABLE_NAME_DEPARTMENTS=$(TABLE_NAME_DEPARTMENTS),\
# spark.yarn.appMasterEnv.TABLE_NAME_PROFESSIONS=$(TABLE_NAME_PROFESSIONS),\
# spark.yarn.appMasterEnv.TABLE_NAME_SYMPTOMS=$(TABLE_NAME_SYMPTOMS),\
# spark.yarn.appMasterEnv.TABLE_NAME_CHRONIC=$(TABLE_NAME_CHRONIC),\
# spark.yarn.appMasterEnv.DQ_MIN_AGE=$(DQ_MIN_AGE),\
# spark.yarn.appMasterEnv.DQ_MAX_AGE=$(DQ_MAX_AGE),\
# spark.yarn.appMasterEnv.DQ_MIN_TEMP=$(DQ_MIN_TEMP),\
# spark.yarn.appMasterEnv.DQ_MAX_TEMP=$(DQ_MAX_TEMP),\
# spark.yarn.appMasterEnv.NAME_REF_JOB=$(NAME_REF_JOB)

# # Подготовка для отправки в спарк (не ошибка)
# empty :=
# space := $(empty) $(empty)
# SPARK_ENV_PROPS = $(subst $(space),,$(SPARK_ENV_PROPS_SPACES))

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
	uv run black src/ jobs/ scripts/ tests/ --check

# Сборка стабильного .whl пакета для отправки на кластер Data Proc
build: clean
	uv build --wheel

# Очистка репозитория от временного мусора, кэша тестов и логов
clean:
	rm -rf src/sparkmedanalytics.egg-info/ dist/ .pytest_cache/ .uv/ uv.lock spark-warehouse/ metastore_db/ derby.log
	find . -name "__pycache__" -exec rm -rf {} +
	find . -name "*.pyc" -exec rm -f {} +

# Обновление справочников и отправка статических файлов в облако
refs-dev: build
# 	yc storage s3 cp data/10_patient_visits_1m.json s3://$(S3_BRONZE_PATH)/patient_visits_1m.json &&
	yc storage s3 cp data/departments.csv s3://$(S3_BRONZE_PATH)/departments.csv && \
	yc storage s3 cp data/professions.csv s3://$(S3_BRONZE_PATH)/professions.csv && \
	yc storage s3 cp jobs/load_references.py s3://$(CODE_BUCKET)/load_references.py && \
	yc storage s3 cp dist/$(WHL_FILE) s3://$(CODE_BUCKET)/$(WHL_FILE) && \
	yc dataproc job create-pyspark \
		--cluster-name $(CLUSTER_NAME) \
		--name $(NAME_REF_JOB) \
		--main-python-file-uri s3a://$(CODE_BUCKET)/load_references.py \
		--python-file-uris s3a://$(CODE_BUCKET)/$(WHL_FILE) \
		--properties spark.submit.pyFiles=s3a://$(CODE_BUCKET)/$(WHL_FILE) \
		$(BASE_PROPERTIES) $(SPARK_ENV_PROPS) \
		$(REPOSITORIES) $(PACKAGES) \
		--async=false

# Пробный запуск ETL джобы на dev s3
silver-dev: test lint build
	yc storage s3 cp jobs/bronze_to_silver.py s3://$(CODE_BUCKET)/bronze_to_silver.py &&
	yc storage s3 cp dist/$(WHL_FILE) s3://$(CODE_BUCKET)/$(WHL_FILE) &&
	yc dataproc job create-pyspark \
		--cluster-name $(CLUSTER_NAME) \
		--name silver-dev \
		--main-python-file-uri s3a://$(CODE_BUCKET)/bronze_to_silver.py \
		--python-file-uris s3a://$(CODE_BUCKET)/$(WHL_FILE) \
		--properties spark.submit.pyFiles=s3a://$(CODE_BUCKET)/$(WHL_FILE) \
		$(BASE_PROPERTIES) $(SPARK_ENV_PROPS) \
		$(REPOSITORIES) $(PACKAGES) \
		--async=false
	
gold-dev:
	echo $(SPARK_ENV_PROPS)
	echo " "
	echo $(spark.yarn.appMasterEnv.NAME_REF_JOB)