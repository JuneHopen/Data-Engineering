from datetime import datetime,date
import re

def validate_date(string_date):

    if not string_date:
        return None

    try:
        return datetime.strptime(string_date, '%Y-%m-%d').date()
    except ValueError:
        raise ValueError(f"Date format must be YYYY-MM-DD for '{string_date}'")

def validate_patient_data(raw_data):
    valid_data = []
    invalid_data = []


    for record_validate in raw_data:
        processed_record = record_validate.copy()
        is_valid = True
        rejection_reasons = []

        if not processed_record['patient_id'] or not re.match(r'^PT-\d{3}$', processed_record['patient_id']):
            is_valid = False
            rejection_reasons.append(f"patient_id invalid format")

        if not processed_record['patient_name']:
            is_valid = False
            rejection_reasons.append(f"patient_name cannot be null")

        try:
            age = int(processed_record['age'])
            processed_record['age'] = age
            if age < 0 or age > 120:
                is_valid = False
                rejection_reasons.append(f"age not realistic")
        except ValueError:
            is_valid = False
            rejection_reasons.append(f"Age value error")

        if processed_record['gender'] not in ['F', 'M']:
            is_valid = False
            rejection_reasons.append(f"Go to hell disease")

        admission_date = None
        discharge_date = None

        if not processed_record['admission_date']:
            is_valid = False
            rejection_reasons.append(f"admission_date must be filled")
        else:
            try:
                admission_date = validate_date(processed_record['admission_date'])
                processed_record['admission_date'] = admission_date
            except ValueError as e:
                is_valid = False
                rejection_reasons.append(f"admission_date: {e}")

        if not processed_record['discharge_date']:
            is_valid = False
            rejection_reasons.append(f"discharge_date must be filled")
        else:
            try:
                discharge_date = validate_date(processed_record['discharge_date'])
                processed_record['discharge_date'] = discharge_date
            except ValueError as e:
                is_valid = False
                rejection_reasons.append(f"discharge_date: {e}")

        if admission_date and discharge_date:
            if discharge_date < admission_date:
                is_valid = False
                rejection_reasons.append(f"discharge_date cannot be before admission_date")
        try:
            treatment_cost = float(processed_record['treatment_cost'])
            if treatment_cost <= 0:
                rejection_reasons.append(f"treatment_cost must be paid")
                is_valid = False
        except ValueError:
            is_valid = False
            rejection_reasons.append(f"treatment_cost invalid format")
        if not re.match(r'\b\d{2,3}/\d{2}\b', processed_record['blood_pressure']):
            is_valid = False
            rejection_reasons.append(f"blood_pressure invalid format")

        try:
            temperature = float(processed_record['temperature'])
            if temperature < 35.0 or temperature > 42.0:
                is_valid = False
                rejection_reasons.append(f"temperature invalid format")
        except ValueError:
            is_valid = False
            rejection_reasons.append('temperature invalid format')

        if is_valid:
            valid_data.append(processed_record)
        else:
            processed_record['rejection_reasons'] = ', '.join(rejection_reasons)
            if isinstance(processed_record['admission_date'], date):
                processed_record['admission_date'] = processed_record['admission_date'].strftime('%Y-%m-%d')
            if isinstance(processed_record['discharge_date'], date):
                processed_record['discharge_date'] = processed_record['discharge_date'].strftime('%Y-%m-%d')
            invalid_data.append(processed_record)
    return valid_data, invalid_data

if __name__ == '__main__':
    from extract import extract_patient_data

    data = extract_patient_data('../data/patient_record.csv')
    cleaned, rejected = validate_patient_data(data)
    for record in cleaned:
        print(f"clean data: {record} records")
    for record in rejected:
        print(f"rejected data: {record}")