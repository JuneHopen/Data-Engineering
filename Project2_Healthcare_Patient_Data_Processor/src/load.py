from datetime import datetime, date
import json
import re
import logging

medical_metadata = {
        "facility_name": "HealthCare Plus Hospital",
        "department": "Medical Records",
        "data_sensitivity": "Confidential - HIPAA Protected",
        "hipaa_compliant": True,
        "data_retention_years": 7,  # Minimum medical records retention
        "clinical_data_quality": "Audited",
        "patient_safety_reviewed": True
    }


def data_quality_score(record):

    null = 0
    not_null = 0
    for head, value in record.items():
        if value is None or value == "" or (isinstance(value, str) and value.strip() == ""):
            null += 1
        else:
            not_null += 1
    total_fields_checked = null + not_null
    score = (not_null - null) / total_fields_checked

    return round(score,3)

def save_patient_records(transformed_data, input_file, output_file):

    try:
        metadata_patient = {
            'metadata': {
                'export_timestamp': datetime.now().isoformat(),
                'total_record': len(transformed_data),
                'data_source': '',
                'facility_name': '',
                'department': '',
                'data_sensitivity': '',
                'processing_version': '1.0',
                'data_retention_years': None
            },
            'patient_records': []
        }

        match_data_source = re.search(r"[a-zA-Z0-9_-]+\.csv", input_file)
        if match_data_source:
            metadata_patient['metadata']['data_source'] = match_data_source.group(0)
        else:
            metadata_patient['metadata']['data_source'] = 'File not found'

        for head, value in medical_metadata.items():
            for record in metadata_patient['metadata']:
                if record in head:
                    metadata_patient['metadata'][record] = value
                    break

        for record in transformed_data:
            final_score = data_quality_score(record)
            metadata_patient['patient_records'].append({
                'patient_id': record['patient_id'],
                'patient_name': record['patient_name'],
                'demographics': {
                    'age': record['age'],
                    'age_group': record['age_group'],
                    'gender': record['gender']
                },
                'medical_info': {
                    'diagnosis': record['diagnosis'],
                    'blood_pressure': record['blood_pressure'],
                    'bp_category': record['bp_category'],
                    'temperature': record['temperature'],
                    'temperature_status': 'Hypothermia' if record['temperature'] < 35 else 'Fever' if record['temperature'] > 37.5 else 'Low' if 35 <= record['temperature'] <= 36 else 'Normal' if 36 < record['temperature'] <= 37.5 else None
                },
                'treatment_details': {
                    'admission_date': record['admission_date'],
                    'discharge_date': record['discharge_date'],
                    'length_of_stay': record['length_of_stay'],
                    'doctor': record['doctor'],
                    'treatment_cost': record['treatment_cost'],
                    'cost_category': record['cost_category']
                },
                'processing_info': {
                    'processed_at': datetime.now().isoformat(),
                    'data_quality_score': 'Excellent' if final_score >= 1 else 'Bad'
                }
            })


        with open(output_file, 'w', encoding='utf-8') as file:
            json.dump(metadata_patient, file ,indent=2)
        return True
    except Exception as e:
        logging.error(f"Error: {e}")
        return False

def generate_medical_report(all_data, valid_data, invalid_data, input_file, report_file):
    try:
        data_quality = round(len(valid_data) / (len(valid_data) + len(invalid_data)) * 100,2)

        data_raw = all_data.copy()
        age = int(data_raw['age'])
        data_raw['age'] = age

        valid_age = 0
        valid_gender = 0
        valid_name = 0
        valid_blood_pressure = 0
        valid_temperature = 0
        valid_clinically_plausible = 0
        valid_date_sequency = 0
        valid_treatment_cost = 0
        valid_doctor_assignment = 0

        invalid_age = 0
        invalid_treatment_cost = 0

        for record in all_data:
            print(type(record['age']))

        # for record in all_data:
        #     age, temperature, treatment_cost, bp_match = -1, -1.0, -1.0, None
        #     try:
        #         age = int(record['age'])
        #         bp_match = re.match(r'(\b\d{2,3})/\d{2,3}\b', record['blood_pressure'])
        #         temperature = float(record['temperature'])
        #         admission_date_obj = datetime.strptime(record['admission_date'], '%Y-%m-%d')
        #         discharge_date_obj = datetime.strptime(record['discharge_date'], '%Y-%m-%d')
        #
        #         if discharge_date_obj >= admission_date_obj:
        #             valid_date_sequency += 1
        #
        #         treatment_cost = float(record['treatment_cost'])
        #
        #     except (ValueError, TypeError):
        #         pass
        #
        #     if 120 > age > 0:
        #         valid_age += 1
        #     else:
        #         invalid_age += 1
        #
        #     gender = record['gender']
        #     if isinstance(gender, str) and gender.upper() in ['F', 'M']:
        #         valid_gender += 1
        #
        #     patient_name = record['patient_name']
        #     if isinstance(patient_name, str) and patient_name.strip():
        #         valid_name += 1
        #
        #     if bp_match:
        #         valid_blood_pressure += 1
        #
        #     if 42 > temperature > 35:
        #         valid_temperature += 1
        #
        #     if 120 > age > 0 and bp_match and 42 > temperature > 35:
        #         valid_clinically_plausible += 1
        #
        #     if treatment_cost > 0:
        #         valid_treatment_cost += 1
        #     else:
        #         invalid_treatment_cost += 1
        #
        #     doctor_assignment = record['doctor']
        #     if doctor_assignment:
        #         valid_doctor_assignment += 1







        metadata_report = {
            'report_metadata': {
            'generated_at': datetime.now().isoformat(),
            'facility': medical_metadata['facility_name'],
            'report_type': 'Medical Data Quality '+ medical_metadata['clinical_data_quality'],
            'reporting_period': datetime.now().strftime('%B %Y'),
            'data_source': ''
            },
            'executive_summary': {
                'total_records_processed': len(valid_data) + len(invalid_data),
                'valid_medical_records': len(valid_data),
                'invalid_medical_records': len(invalid_data),
                'data_quality': data_quality,
                'compliance_status': 'Needs Improvement' if data_quality < 50 else 'Excellent data',
                'patient_safety_risk': 'Medium'
            },
            'clinical_data_quality': {
                'demographic_accuracy': {
                    'valid_age_records': valid_age,
                    'valid_gender_records': valid_gender,
                    'complete_patient_names': valid_name
                },
                'vital_signs_quality': {
                    'valid_blood_pressure': valid_blood_pressure,
                    'valid_temperature': valid_temperature,
                    'clinically_plausible_values': valid_clinically_plausible
                },
                'treatment_data_quality': {
                    'valid_date_sequency': valid_date_sequency,
                    'positive_treatment_cost': valid_treatment_cost,
                    'complete_doctor_assignments': valid_doctor_assignment
                }
            },
            'error_analysis': {
                'critical_errors': {
                    'invalid_age_values': invalid_age,
                    'negative_treatment_cost': invalid_treatment_cost,
                    # 'date_sequence_violations': invalid_date
                }
            }
        }


        match_data_source = re.search(r"[a-zA-Z0-9_-]+\.csv", input_file)
        if match_data_source:
            metadata_report['report_metadata']['data_source'] = match_data_source.group(0)
        else:
            metadata_report['report_metadata']['data_source'] = 'File not found'

        with open(report_file, 'w', encoding='utf-8') as file:
            json.dump(metadata_report, file ,indent=2)
        return True
    except Exception as e:
        logging.error(f"Error: {e}")
        return None


if __name__ == '__main__':
    from extract import extract_patient_data
    from validate import validate_patient_data
    from transform import transform_patient_data

    data = extract_patient_data('../data/patient_record.csv')
    cleaned, rejected = validate_patient_data(data)

    transformed = transform_patient_data(cleaned)

    load = save_patient_records(transformed, '../data/patient_record.csv', '../data/clean_records.json')
    print(load)

    report = generate_medical_report(data, cleaned, rejected, '../data/patient_record.csv', '../data/medical_quality_report.json')