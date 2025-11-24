import logging
import csv

def extract_sales_data(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            data = list(reader)

        logging.info(f'Successfully extracted {len(data)} records')
        return data

    except FileNotFoundError:
        logging.error(f"File {file_path} does not exist")
        return []
    except Exception as e:
        logging.error(f"File extraced failed: {e}")
        return []

if __name__ == '__main__':
    data = extract_sales_data('../data/daily_sales.csv')
    print(data)
