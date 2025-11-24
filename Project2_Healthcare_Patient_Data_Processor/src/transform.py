from datetime import datetime
import re

def transform_patient_data(valid_data):
    transformed_data = []

    BP_CATEGORIES = {
        'Normal': (90, 120),  # Systolic
        'Elevated': (120, 130),
        'High Stage 1': (130, 140),
        'High Stage 2': (140, 180),
        'Hypertensive Crisis': (180, 300)
    }

    for record_data in valid_data:
        try:
            age = int(record_data['age'])
            record_data['age'] = age
            treatment_cost =  float(record_data['treatment_cost'])
            temperature = float(record_data['temperature'])

            record_data['length_of_stay'] = (record_data['discharge_date'] - record_data['admission_date']).days
            record_data['admission_date'] = record_data['admission_date'].strftime('%Y-%m-%d')
            record_data['discharge_date'] = record_data['discharge_date'].strftime('%Y-%m-%d')

            record_data['age_group'] = 'Child' if age < 18 else 'Adult' if age <= 65 else 'Senior'

            blood_pressure_str = record_data['blood_pressure']
            match = re.match(r'(\b\d{2,3})/\d{2,3}\b', blood_pressure_str)

            if match:
                systolic_pressure = int(match.group(1))
                found_category = None

                for main_category, (min_bp, max_bp) in BP_CATEGORIES.items():
                    if min_bp <= systolic_pressure < max_bp:
                        found_category = main_category
                        break
                record_data['bp_category'] = found_category
            else:
                record_data['bp_category'] = 'Invalid'

            cost = int(record_data['treatment_cost'])
            record_data['cost_category'] = 'Low' if cost < 1000000 else 'Medium' if cost <= 2000000 else 'High'

            record_data['treatment_cost'] = treatment_cost
            record_data['temperature'] = temperature

            transformed_data.append(record_data)

        except ValueError:
            return False
    return transformed_data

if __name__ == '__main__':
    from extract import extract_patient_data
    from validate import validate_patient_data

    data = extract_patient_data('../data/patient_record.csv')
    cleaned, rejected = validate_patient_data(data)

    transformed = transform_patient_data(cleaned)
    for record in transformed:

        print(record)