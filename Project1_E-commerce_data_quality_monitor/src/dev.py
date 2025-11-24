import json
from datetime import datetime
import logging
import re

def save_clean_data(transformed_data, output_file):
    try:
        load_info = {
            'metadata': {
                'export_timestamp': datetime.now().isoformat(),
                'total_records': len(transformed_data),
                'data_source': 'daily_sales.csv',
                'processing_version': '1.0'
            },
            'sales_data': transformed_data
        }

        with open(output_file, 'w', encoding='utf-8') as file:
            json.dump(load_info, file, indent=2)
        logging.info(f"Successfully loaded {len(transformed_data)} records")
        return True

    except Exception as e:
        logging.error(f"Error loading data: {e}")
        return False

def generate_quality_report(valid_data, invalid_data, input_file, report_file):
    success_rate = round(len(valid_data) / (len(valid_data) + len(invalid_data)) * 100, 2)
    quality_report = {
        'report_metadata': {
            'generated_at': datetime.now().isoformat(),
            'data_source': '',
            'report_type': 'data_quality'
        },
        'summary': {
            'total_records_processed': len(valid_data) + len(invalid_data),
            'valid_records': len(valid_data),
            'invalid_records': len(invalid_data),
            'success_rate': success_rate,
            'data_quality_grade': 'A' if success_rate >= 90 else 'B' if success_rate >= 80 else "C" if success_rate >= 70 else 'D' if success_rate >= 60 else "E"
        },
        'validation_details': {
            'error_breakdown': {},
            'invalid_records': []
        },
        'recommendations': []
    }

    match = re.search(r'([^/]+)$', input_file)
    if match:
        quality_report['report_metadata']['data_source'] = match.group(0)

    for record in invalid_data:
        reason = record['rejection_reasons']
        quality_report['validation_details']['error_breakdown'][reason] = quality_report['validation_details']['error_breakdown'].get(reason, 0) + 1

        quality_report['validation_details']['invalid_records'].append({
            'order_id': record['order_id'],
            'error_reasons': reason,
            'original_data': record
        })

        if 'customer_name' in  record['rejection_reasons']:
            quality_report['recommendations'].append("Improve data entry validation for customer names")
        if 'quantity' in record['rejection_reasons']:
            quality_report['recommendations'].append("Add quantity validation in sales system")
        if 'order_date' in record['rejection_reasons']:
            quality_report['recommendations'].append("Fix date picker in order management system")





        # quality_report['invalid_records'].append(invalid_data)

    with open(report_file, 'w', encoding='utf-8') as file:
        json.dump(quality_report, file, indent=2)




if __name__ == "__main__":
    from extract import extract_sales_data
    from validate import validate_sales_data
    from transform import transform_sales_data

    # Test the complete flow
    raw_data = extract_sales_data("../data/daily_sales.csv")
    cleaned, rejected = validate_sales_data(raw_data)
    final_data = transform_sales_data(cleaned)


    # Load cleaned data
    success = save_clean_data(final_data, '../data/clean_sales.json')
    print(f"Load successful: {success}")

    generate_quality_report(cleaned, rejected, "../data/daily_sales.csv", '../data/quality_report.json')