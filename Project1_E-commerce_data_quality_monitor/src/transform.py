from datetime import datetime

def transform_sales_data(valid_data):
    transformed_data = []

    category_map = {
        'Electronics': {
            'laptop': 'Laptop',
            'wireless mouse': 'Wireless Mouse',
            'notebook': 'Notebook',
            'keyboard': 'Keyboard',
            'desk lamp': 'Desk Lamp'
        },
        'Books': {
            'programming book': 'Programming Book'
        }
    }

    for record in valid_data:
        try:
            price = float(record['price'])
            quantity = int(record['quantity'])
            total_amount = round(price * quantity, 2)

            product_name = record['product'].lower()
            found_category = 'Unknown'

            for main_category, sub_category in category_map.items():
                if product_name in sub_category:
                    found_category = main_category
                    break

            transformed_record = {
                'order_id': record['order_id'],
                'customer_name': record['customer_name'],
                'product': record['product'],
                'price': price,
                'quantity': quantity,
                'total_amount': total_amount,
                'category': found_category,
                'order_date': record['order_date'],
                'region': record['region'],
                'processed_date': datetime.now().isoformat()
            }

            transformed_data.append(transformed_record)

        except ValueError:
            return False

    return transformed_data

if __name__ == '__main__':
    from extract import extract_sales_data
    from validate import validate_sales_data

    data = extract_sales_data('../data/daily_sales.csv')

    cleaned, rejected = validate_sales_data(data)

    print(transform_sales_data(cleaned))