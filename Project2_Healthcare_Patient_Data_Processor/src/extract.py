import logging
import csv

def extract_patient_data(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            data = list(reader)
        logging.info(f"Successfully extract {len(data)} records")
        return data
    except FileNotFoundError:
        logging.error(f"File {file} not found")
        return False
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        return []

if __name__ == '__main__':
    extraction = extract_patient_data('../data/patient_record.csv')
    print(extraction)