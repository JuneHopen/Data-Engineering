import logging
import re
from extract import extract_sales_data
from validate import validate_sales_data
from transform import transform_sales_data
from load import save_clean_data,generate_quality_report

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

def run_etl_pipeline():

    input_file = '../data/daily_sales.csv'
    output_file = '../data/clean_sales.json'
    report_file = '../data/quality_report.json'

    try:
        logging.info('=== SALES DATA PIPELINE STARTED ===\n📁 STEP 1: Extracting data...')
        raw_data = extract_sales_data(input_file)

        match = re.search(r'([^/]+)$', input_file)
        file = ''

        if match:
            file = match.group(0)

        if not raw_data:
            logging.error('No data extracted. Pipeline stopped')
            return False
        else:
            logging.info(f'✅ Extracted {len(raw_data)} records from {file}')

        cleaned, rejected = validate_sales_data(raw_data)

        if not cleaned:
            logging.warning('No valid data')

        logging.info(f'valid records: {len(cleaned)}')
        logging.info(f'invalid records: {len(rejected)}')
        for record in rejected:
            logging.info(f" - {record['order_id']} : {record['rejection_reasons']}")

        logging.info('🔄 STEP 3: Transforming data...')
        transformed = transform_sales_data(cleaned)
        if not transformed:
            logging.warning(f"No data transformed")
        logging.info(f"Transformed {len(transformed)} records")

        # LOAD
        logging.info("Step 3. Loading data...")
        load_success = save_clean_data(transformed, output_file)
        logging.info(f"✅ Saved clean data to: clean_sales.json ({len(transformed)} records)")


        # GENERATE REPORT
        logging.info("Step 4. Generating report...")
        generate_quality_report(cleaned, rejected, input_file, report_file)
        logging.info(f"✅ Generated quality report: quality_report.json")

        logging.info(f"=== PIPELINE COMPLETED ===")
        success_rate = round(len(cleaned) / (len(cleaned) + len(rejected)) * 100, 2)
        grade = 'A' if success_rate >= 90 else 'B' if success_rate >= 80 else "C" if success_rate >= 70 else 'D' if success_rate >= 60 else "E"
        logging.info(f"Data Quality Score: {success_rate}% (Grade {grade}) ")

        logging.info(f"Valid: {len(transformed)} records | Invalid: {len(rejected)}")
        return load_success


    except Exception as e:
        logging.error(f"ETL Pipeline failed: {e}")
        return False

if __name__ == '__main__':
    success = run_etl_pipeline()

