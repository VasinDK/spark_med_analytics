# Спецификация данных проекта SparkMedAnalytics

Документ фиксирует схему данных JSON объекта массива истории болезни

## 📊 Маппинг полей и типов данных

| Имя поля | Описание | Тип в JSON (S3) | Тип в Spark / Iceberg | Тип в ClickHouse | Пример |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `id` | Уникальный идентификатор записи | `Number` | `Long` | `UInt64` | `94821` |
| `visit_date` | Дата визита пациента | `String` | `Date` | `Date` | `"2026-05-14"` |
| `age` | Возраст пациента | `Number` | `Integer` | `UInt8` | `34` |
| `gender_id` | Код пола (ISO/IEC 5218) | `Number` | `Integer` | `UInt8` | `2` |
| `profession_id` | ID профессии из справочника | `Number` | `Integer` | `UInt32` | `105` |
| `doctor_id` | ID лечащего врача | `Number` | `Integer` | `UInt16` | `42` |
| `department_id` | ID отделения клиники | `Number` | `Integer` | `UInt16` | `3` |
| `snils` | СНИЛС (формат: XXX-XXX-XXX XX) | `String` | `String` | `String` | `"142-351-802 44"` |
| `height` | Рост в сантиметрах | `Number` | `Integer` | `UInt8` | `165` |
| `weight` | Вес в килограммах | `Number` | `Float` | `Float32` | `62.0` |
| `temperature` | Температура тела (Цельсий) | `Number` | `Float` | `Float32` | `38.5` |
| `bp_systolic` | Верхнее давление (систолическое) | `Number` | `Integer` | `UInt8` | `110` |
| `bp_diastolic` | Нижнее давление (диастолическое) | `Number` | `Integer` | `UInt8` | `70` |
| `disease_code` | Основной диагноз по МКБ-10 | `String` | `String` | `LowCardinality(String)` | `"J06.9"` |
| `blood_type` | Группа крови и резус-фактор | `String` | `String` | `LowCardinality(String)` | `"A+"` |
| `symptoms_code` | Массив кодов текущих симптомов | `Array(String)` | `Array(String)`| `Array(String)` | `["R50", "R05"]` |
| `chronic_diseases`| Массив кодов хрон. заболеваний | `Array(String)` | `Array(String)`| `Array(String)` | `["E11"]` |
| `lab_hemoglobin` | Уровень гемоглобина в крови | `Number` | `Float` | `Float32` | `130.2` |
| `lab_leukocytes` | Уровень лейкоциты в крови | `Number` | `Float` | `Float32` | `11.5` |
| `lab_glucose` | Уровень глюкозы в крови | `Number` | `Float` | `Float32` | `4.8` |
| `lab_cholesterol`| Уровень общего холестерина | `Number` | `Float` | `Float32` | `4.2` |

## 📋 Пример записи истории болезни

```json
[
  {
    "id": 94821,
    "visit_date": "2026-05-14",
    "age": 34,
    "gender_id": 2,
    "profession_id": 105,
    "doctor_id": 42,
    "department_id": 3,
    "snils": "142-351-802 44",
    "height": 165,
    "weight": 62.0,
    "temperature": 38.5,
    "bp_systolic": 110,
    "bp_diastolic": 70,
    "disease_code": "J06.9",
    "blood_type": "A+",
    "symptoms_code": ["R50", "R05", "R51"],
    "chronic_diseases": ["J45.0"],
    "lab_hemoglobin": 130.2,
    "lab_leukocytes": 11.5,
    "lab_glucose": 4.8,
    "lab_cholesterol": 4.2
  }
]
```

