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

def convert_value(value):
    try:
        decimal = float(value)

        if decimal.is_integer():
            return int(decimal)
        else:
            return decimal

    except (ValueError, TypeError):
        pass

    try:
        return datetime.strptime(value, '%Y-%m-%d')
    except (ValueError, TypeError):
        pass

    return value


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
        data_quality = round(len(valid_data) / (len(valid_data) + len(invalid_data)) * 100,2)


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
        invalid_date_sequency = 0
        invalid_patient_name = 0
        invalid_date_format = 0
        invalid_discharge_date = 0


        for record in all_data:
            for key, value in record.items():
                new_record = convert_value(value)
                record[key] = new_record


            age = record['age']
            if 120 > age > 0:
                valid_age += 1
            else:
                invalid_age += 1

            gender = record['gender']
            if gender.upper() in ['F', 'M']:
                valid_gender += 1

            if not record['patient_name']:
                invalid_patient_name += 1
            else:
                valid_name += 1

            bp_match = re.match(r'(\b\d{2,3})/\d{2,3}\b', record['blood_pressure'])
            if bp_match:
                valid_blood_pressure += 1

            if 42 > record['temperature'] > 35:
                valid_temperature += 1

            if 120 > age > 0 and bp_match and 42 > record['temperature'] > 35:
                valid_clinically_plausible += 1

            admission_date = record['admission_date']
            discharge_date = record['discharge_date']

            if not isinstance(admission_date, datetime) or not isinstance(discharge_date, datetime) or isinstance(admission_date, str) or isinstance(discharge_date, str):
                invalid_date_format += 1

            if isinstance(discharge_date, str):
                invalid_discharge_date =+ 1

            if isinstance(admission_date, datetime) and isinstance(discharge_date, datetime):
                if discharge_date >= admission_date:
                    valid_date_sequency += 1
                else:
                    invalid_date_sequency += 1
            else:
                pass


            if record['treatment_cost'] > 0:
                valid_treatment_cost += 1
            else:
                invalid_treatment_cost += 1

            if not record['doctor']:
                pass
            else:
                valid_doctor_assignment += 1




        invalid_records_detail = []
        for invalid_record in invalid_data:
            admission_date = invalid_record['admission_date']
            discharge_date = invalid_record['discharge_date']

            for key, value in invalid_record.items():
                new_record = convert_value(value)
                invalid_record[key] = new_record

            age = invalid_record.get('age', -1)
            patient_name = invalid_record.get('patient_name', '')
            treatment_cost = invalid_record.get('treatment_cost', -1)
            admission_date = invalid_record['admission_date']
            discharge_date = invalid_record['discharge_date']

            is_high_risk = not patient_name.strip() or not (isinstance(age, int) and 120 > age > 0)
            is_medium_risk = not (isinstance(treatment_cost, int) and treatment_cost > 0)

            errors_list = []

            # Menambahkan detail error spesifik (opsional)
            if not patient_name.strip():
                errors_list.append("Missing patient name")
            if not (isinstance(age, int) and 120 > age > 0):
                errors_list.append("Implausible age")
            if isinstance(admission_date, datetime) and isinstance(discharge_date,
                                                                   datetime) and discharge_date < admission_date:
                errors_list.append("Discharge date before admission")
            if not (isinstance(treatment_cost, int) and treatment_cost > 0):
                errors_list.append("Negative or zero treatment cost")

            if is_high_risk:
                severity = 'High'
                impact = 'Patient Identification risk/Clinical decision risk'
                action = 'Urgent data correction found'
            elif is_medium_risk:
                severity = 'Medium'
                impact = 'Billing inaccuracy'
                action = 'Review Billing System'
            else:
                severity = 'Low'
                impact = 'Administrative risk'
                action = 'Contact medical records department'

            invalid_records_detail.append({
                'patient_id': invalid_record.get('patient_id', 'N/A'),
                'error_severity': severity,
                'clinical_impact': impact,
                'errors': errors_list if errors_list else [invalid_record.get('rejection_reasons', 'N/A')],
                'recommended_action': action
            })

            clinical_accuracy_score = round(
                (valid_age + valid_temperature + valid_blood_pressure) / (len(all_data) * 3) * 100, 1)
            data_completeness_score = round(
                (valid_name + valid_doctor_assignment + valid_treatment_cost) / (len(all_data) * 3) * 100, 1)

            actionable_recommendation = []
            if invalid_age > 0:
                actionable_recommendation.append({
                    'priority': 'High',
                    'recommendation': 'Implement age validation in patient registration system',
                    "department": "IT/Medical Records",
                    "timeline": "Immediate"
                })

            if invalid_date_format > 0 or invalid_date_sequency > 0:
                actionable_recommendation.append({
                    "priority": "High",
                    "recommendation": "Fix date validation in admission/discharge system",
                    "department": "IT/Admissions",
                    "timeline": "1 week"
                })

            if invalid_treatment_cost > 0:
                actionable_recommendation.append({
                    "priority": "Medium",
                    "recommendation": "Add cost validation in billing system",
                    "department": "Finance/Billing",
                    "timeline": "2 weeks"
                })

            if data_quality < 50:
                actionable_recommendation.append({
                    "priority": "Medium",
                    "recommendation": "Staff training on data entry protocols",
                    "department": "HR/Medical Staff",
                    "timeline": "1 month"
                })




            metadata_report = {
                'report_metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'facility': medical_metadata['facility_name'],
                    'report_type': 'Medical Data Quality ' + medical_metadata['clinical_data_quality'],
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
                        'date_sequence_violations': invalid_date_sequency
                    },
                    'data_integrity_issue': {
                        'missing_patient_name': invalid_patient_name,
                        'invalid_date_format': invalid_date_format,
                        'missing_discharge_dates': invalid_discharge_date
                    },
                    'invalid_record_detail': invalid_records_detail
                },
                'compliance_metrics': {
                    'hipaa_compliance_score': 0,
                    'data_completeness_score': data_completeness_score,
                    'clinical_accuracy_score': clinical_accuracy_score
                },
                'actionable_recommendation': actionable_recommendation,
                "patient_safety_indicators": {
                    "records_with_clinical_risk": 2,
                    "potential_medication_errors": 0,
                    "data_issues_affecting_care": 1
                }
            }
                # if value in contact_medical:

        match_data_source = re.search(r"[a-zA-Z0-9_-]+\.csv", input_file)
        if match_data_source:
            metadata_report['report_metadata']['data_source'] = match_data_source.group(0)
        else:
            metadata_report['report_metadata']['data_source'] = 'File not found'

        with open(report_file, 'w', encoding='utf-8') as file:
            json.dump(metadata_report, file ,indent=2)
        return True


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