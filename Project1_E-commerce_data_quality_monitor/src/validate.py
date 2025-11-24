import logging
import re
from calendar import prcal
from datetime import datetime

def validate_date(date_string):
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def validate_sales_data(raw_data):
    valid_data = []
    invalid_data = []

    for record in raw_data:
        processed_data = record.copy()
        is_valid = True
        rejection_reasons = []

        if not processed_data['customer_name']:
            is_valid = False
            rejection_reasons.append('customer_name empty')

        if not validate_date(processed_data['order_date']):
            is_valid = False
            rejection_reasons.append('order_date invalid')

        try:
            quantity = float(processed_data['quantity'])

            if quantity <= 0:
                is_valid = False
                rejection_reasons.append('quantity invalid')
        except ValueError:
            is_valid = False
            rejection_reasons.append('Invalid numeric format')

        if is_valid:
            valid_data.append(processed_data)
        else:
            processed_data['rejection_reasons'] = '. '.join(rejection_reasons)
            invalid_data.append(processed_data)
    return valid_data, invalid_data

if __name__ == '__main__':
    from extract import extract_sales_data

    data = extract_sales_data('../data/daily_sales.csv')

    cleaned, rejected = validate_sales_data(data)

    print(f"Cleaned data: {len(cleaned)} records")
    print(f"Rejected data: {len(rejected)} records")

    print(f"Valid data: \n{cleaned}")
    print(f"\nInvalid data:\n{rejected}")




