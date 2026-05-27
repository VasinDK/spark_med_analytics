import os
import random
from datetime import datetime, timedelta

def generate_medical_dataset(output_filepath: str, num_records: int = 10):
    """
    Генерирует JSON-файл, содержащий массив из num_records медицинских записей.
    Использует потоковую запись для экономии оперативной памяти (RAM).
    """
    # Справочники для генерации реалистичных медицинских данных
    disease_codes = ["J06.9", "I10", "E11.9", "I25.1", "J45.0", "K29.7", "M17.9", "N39.0"]
    blood_types = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
    symptoms_pool = ["R50", "R05", "R51", "R53", "R07.4", "R10.4", "R06.0", "R21"]
    chronic_pool = ["E11", "J45", "I10", "I25", "K29", "M17"]
    
    start_date = datetime(2026, 1, 1)
    
    print(f"Начало генерации {num_records} записей в файл: {output_filepath}")
    
    # Открываем файл на запись с буферизацией строк
    with open(output_filepath, 'w', encoding='utf-8', buffering=64*1024) as f:
        # Открываем квадратную скобку JSON-массива
        f.write("[\n")
        
        for i in range(num_records):
            # 1. Генерация базовых ID и демографии
            record_id = 100000 + i
            age = random.randint(18, 90)
            gender_id = random.choice([1, 2]) # 1 - М, 2 - Ж
            profession_id = random.randint(100, 150)
            doctor_id = random.randint(1, 200)
            department_id = random.randint(1, 10)
            
            # Генерация СНИЛС формата "XXX-XXX-XXX XX"
            snils = f"{random.randint(100,999):03d}-{random.randint(100,999):03d}-{random.randint(100,999):03d} {random.randint(0,99):02d}"
            
            # Генерация случайной даты визита за последние 130 дней
            visit_date = (start_date + timedelta(days=random.randint(0, 130))).strftime("%Y-%m-%d")
            
            # 2. Физические показатели со случайными аномалиями (симптомами)
            has_fever = random.random() < 0.15
            temperature = round(random.uniform(38.0, 40.5), 1) if has_fever else round(random.uniform(36.2, 37.2), 1)
            
            height = random.randint(155, 195) if gender_id == 1 else random.randint(145, 180)
            weight = round(random.uniform(60.0, 110.0), 1) if gender_id == 1 else round(random.uniform(45.0, 90.0), 1)
            
            bp_systolic = random.randint(110, 150)
            bp_diastolic = random.randint(70, 95)
            
            # 3. Медицинские коды и массивы
            disease_code = random.choice(disease_codes)
            blood_type = random.choice(blood_types)
            
            # Случайное количество симптомов (от 1 до 3) и хроники (от 0 до 2)
            symptoms = random.sample(symptoms_pool, k=random.randint(1, 3))
            if has_fever and "R50" not in symptoms:
                symptoms.append("R50") # Добавляем код лихорадки, если температура повышена
                
            chronic = random.sample(chronic_pool, k=random.randint(0, 2))
            
            # 4. Лабораторные анализы (Генерация Number-значений)
            lab_hemoglobin = round(random.uniform(110.0, 170.0), 1)
            lab_leukocytes = round(random.uniform(4.0, 15.0), 1)
            lab_glucose = round(random.uniform(3.5, 8.5), 1)
            lab_cholesterol = round(random.uniform(3.0, 7.0), 1)
            
            # Формируем JSON-строку вручную, чтобы контролировать типы (Number и String)
            symptoms_str = ", ".join([f'"{s}"' for s in symptoms])
            chronic_str = ", ".join([f'"{c}"' for c in chronic])
            
            json_record = (
                f'  {{\n'
                f'    "id": {record_id},\n'
                f'    "visit_date": "{visit_date}",\n'
                f'    "age": {age},\n'
                f'    "gender_id": {gender_id},\n'
                f'    "profession_id": {profession_id},\n'
                f'    "doctor_id": {doctor_id},\n'
                f'    "department_id": {department_id},\n'
                f'    "snils": "{snils}",\n'
                f'    "height": {height},\n'
                f'    "weight": {weight},\n'
                f'    "temperature": {temperature},\n'
                f'    "bp_systolic": {bp_systolic},\n'
                f'    "bp_diastolic": {bp_diastolic},\n'
                f'    "disease_code": "{disease_code}",\n'
                f'    "blood_type": "{blood_type}",\n'
                f'    "symptoms_code": [{symptoms_str}],\n'
                f'    "chronic_diseases": [{chronic_str}],\n'
                f'    "lab_hemoglobin": {lab_hemoglobin},\n'
                f'    "lab_leukocytes": {lab_leukocytes},\n'
                f'    "lab_glucose": {lab_glucose},\n'
                f'    "lab_cholesterol": {lab_cholesterol}\n'
                f'  }}'
            )
            
            f.write(json_record)
            
            # Ставим запятую после каждого элемента, кроме последнего
            if i < num_records - 1:
                f.write(",\n")
            else:
                f.write("\n")
                
        # Закрываем квадратную скобку JSON-массива
        f.write("]\n")
        
    file_size_mb = os.path.getsize(output_filepath) / (1024 * 1024)
    print(f"Успешно сгенерировано! Размер файла: {file_size_mb:.2f} MB")

if __name__ == "__main__":
    # Запуск генерации 1 млн строк в текущую директорию
    generate_medical_dataset(output_filepath="data/patient_visits_1m.json", num_records=10)
