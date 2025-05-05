import pandas as pd
import psycopg2
import argparse
import yaml
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("financial_data_processor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("FinancialDataProcessor")

class DatabaseConnector:
    """PostgreSQL database connection manager"""
    
    def __init__(self, config):
        self.config = config
        self.connection = None
        self.cursor = None
        self.table = config.get('table')
    
    def connect(self):
        """Establish connection to database"""
        try:
            self.connection = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password']
            )
            self.cursor = self.connection.cursor()
            logger.info("Database connection established")
            return True
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.commit()
            self.connection.close()
        logger.info("Database connection closed")
    
    def insert_data(self, data_rows):
        """Insert multiple rows into the database table"""
        if not self.connection or not self.cursor:
            logger.error("No active database connection")
            return False
        
        try:
            insert_query = """
            INSERT INTO {table}
            (acc_type, application_id, att_id, att_name, att_value, customer_id, year, year_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """.format(table=self.table)

            self.cursor.executemany(insert_query, data_rows)
            self.connection.commit()
            logger.info(f"Successfully inserted {len(data_rows)} rows")
            return True
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error inserting data: {e}")
            return False


class ExcelProcessor:
    """Processes the Excel file and extracts financial data"""
    
    def __init__(self, excel_path, attribute_config,config):
        self.config = config
        self.excel_path = excel_path
        self.attribute_config = attribute_config
        self.df = None
    
    def load_excel(self):
        """Load the Excel file and focus on Compute sheet"""
        try:
            #Get the sheetname from config if available or treat default as "Compute"
            sheet_name = self.config.get("sheet_name", "Compute")
            self.df = pd.read_excel(self.excel_path, sheet_name=sheet_name)
            logger.info(f"Successfully loaded Excel file: {self.excel_path}")
            return True
        except Exception as e:
            logger.error(f"Error loading Excel file: {e}")
            return False
    
    def extract_metadata(self):
        """Extract company, audit type and years information"""
        metadata = {}
        
        # Extract company name
        company_name_row = self.df.loc[self.df.iloc[:, 0] == "Name of the Company"]
        if not company_name_row.empty:
            metadata['company_name'] = company_name_row.iloc[0, 2]
        
        # Extract years and account types
        year_row_index = self.config.get("year_row", None)
        year_row = self.df.iloc[year_row_index] if year_row_index is not None else None
        
        # Handle account type row - from config or fallback search
        account_type_row_index = self.config.get("account_type_row", None)
        if account_type_row_index is not None:
            account_type_row = self.df.iloc[account_type_row_index]
        else:
            matched_rows = self.df.loc[self.df.iloc[:, 0] == "Type of accounts (Audited or Management)"]
            account_type_row = matched_rows.iloc[0] if not matched_rows.empty else None
        
        metadata['years'] = {}
        if year_row is not None:
            # Start from column 1 where the yearly data begins
            for col in range(1, min(11, len(year_row))):
                try:
                    year_val = year_row.iloc[col]  # Use iloc to access by position
                    if pd.notna(year_val):
                        # Extract year from date string
                        year = int(year_val)
                        acc_type = None
                        
                        if account_type_row is not None:
                            acc_raw = account_type_row.iloc[col] if col < len(account_type_row) else None
                            if pd.notna(acc_raw):
                                acc_type = "audited" if "Audit" in str(acc_raw) else "managed"
                        
                        metadata['years'][col] = {'year': year, 'acc_type': acc_type, 'year_id': col - 1}
                except (ValueError, TypeError) as e:
                    logger.warning(f"Couldn't parse year from {year_val}: {e}")
        
        logger.info(f"Extracted metadata: {metadata}")
        return metadata
    
    def extract_financial_data(self, customer_id, application_id):
        """
        Extract financial data based on attribute configuration
        Returns list of tuples ready for database insertion
        """
        logger.info(f"attribute_config: {self.attribute_config}")

        if self.df is None:
            logger.error("Excel file not loaded")
            return []
        
        data_rows = []
        metadata = self.extract_metadata()
        
        # Process each attribute defined in the configuration
        for attribute in self.attribute_config:
            att_id = attribute['id']
            row_number = attribute['row'] - 2

            #fetching the name from config            
            att_name = attribute.get('name', f"Attribute_{att_id}")

            try:
                # Get the row containing the attribute
                attribute_row = self.df.iloc[row_number]
                
                # Extract values for each year
                for col, year_info in metadata['years'].items():
                    try:
                        
                        value = attribute_row[col]
                        print(f"[DEBUG] Processing '{att_name}' (Row {row_number + 1}), Col {col}: Value = {value}")
                        # Skip if value is NaN or empty
                        if pd.isna(value):
                            continue
                            
                        # Create a database row tuple
                        data_row = (
                            year_info['acc_type'],  # acc_type
                            application_id,         # application_id 
                            att_id,                 # att_id
                            att_name,               # att_name
                            float(value),           # att_value
                            customer_id,            # customer_id
                            year_info['year'],      # year
                            year_info['year_id']    # year_id
                        )
                        data_rows.append(data_row)
                        
                    except Exception as e:
                        logger.warning(f"Error extracting value for attribute {att_name}, year {year_info['year']}: {e}")
                        
            except Exception as e:
                logger.warning(f"Error processing attribute {att_name} at row {row_number}: {e}")
        
        logger.info(f"Extracted {len(data_rows)} data points")
        return data_rows


def load_config(config_path):
    """Load YAML configuration file"""
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        return None


def main():
    """Main entry point for the application"""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Process financial Excel data')
    parser.add_argument('--excel', required=True, help='Path to Excel file')
    parser.add_argument('--config', required=True, help='Path to configuration YAML file')
    parser.add_argument('--customer_id', required=True, type=int, help='Customer ID')
    parser.add_argument('--application_id', required=True, type=int, help='Application ID')
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    if not config:
        return False
    
    # Process Excel file
    excel_processor = ExcelProcessor(args.excel, config['attributes'],config)
    if not excel_processor.load_excel():
        return False
    
    # Extract data
    data_rows = excel_processor.extract_financial_data(args.customer_id, args.application_id)
    if not data_rows:
        logger.warning("No data extracted from Excel")
        return False
    
    # Store data in database
    db_connector = DatabaseConnector(config['database'])
    if not db_connector.connect():
        return False
    
    success = db_connector.insert_data(data_rows)
    db_connector.close()
    
    return success

def process_excel_for_insertion(excel_path, config_path, customer_id, application_id):
    config = load_config(config_path)
    if not config:
        return False

    excel_processor = ExcelProcessor(excel_path, config['attributes'], config)
    if not excel_processor.load_excel():
        return False

    data_rows = excel_processor.extract_financial_data(customer_id, application_id)
    if not data_rows:
        logger.warning("No data extracted from Excel")
        return False

    db_connector = DatabaseConnector(config['database'])
    if not db_connector.connect():
        return False

    success = db_connector.insert_data(data_rows)
    db_connector.close()
    
    return success


if __name__ == "__main__":
    result = main()
    exit_code = 0 if result else 1
    exit(exit_code)