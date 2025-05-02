import pandas as pd
import argparse
import yaml
import logging
import os
import sys
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("financial_data_validator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("FinancialDataValidator")

class ExcelValidator:
    """Validates Excel files according to specified configuration"""
    
    def __init__(self, config):
        self.config = config
        self.df = None
        self.validation_results = {
            "is_valid": True,
            "errors": [],
            "warnings": []
        }
    
    def validate_file(self, file_path):
        """Main validation method"""
        if not self._validate_file_exists(file_path):
            return self.validation_results
            
        if not self._validate_file_extension(file_path):
            return self.validation_results
            
        if not self._load_excel(file_path):
            return self.validation_results
            
        self._validate_sheet_not_empty()
        self._validate_company_name()
        if self.config.get("validate_audit_type", True):
            self._validate_has_audited_values()
        else:
            logger.info("Audit type validation skipped as per configuration")
        self._validate_year_row()
        self._validate_row_names()
        self._validate_row_values()
        
        # Additional validations
        self._validate_column_continuity()
        
        return self.validation_results
    
    def _add_error(self, message):
        """Add error message and mark validation as failed"""
        self.validation_results["errors"].append(message)
        self.validation_results["is_valid"] = False
        logger.error(message)
    
    def _add_warning(self, message):
        """Add warning message without failing validation"""
        self.validation_results["warnings"].append(message)
        logger.warning(message)
    
    def _validate_file_exists(self, file_path):
        """Check if file exists"""
        if not os.path.exists(file_path):
            self._add_error(f"File does not exist: {file_path}")
            return False
        return True
    
    def _validate_file_extension(self, file_path):
        """Check if file has valid extension"""
        _, ext = os.path.splitext(file_path)
        if ext.lower() not in ['.xls', '.xlsx']:
            self._add_error(f"Invalid file format: {ext}. Expected .xls or .xlsx")
            return False
        return True
    
    def _load_excel(self, file_path):
        """Load Excel file and target sheet"""
        try:
            sheet_name = self.config.get("sheet_name", "Compute")
            
            # Choose engine based on file extension
            _, ext = os.path.splitext(file_path)
            engine = "xlrd" if ext.lower() == '.xls' else "openpyxl"
            
            self.df = pd.read_excel(file_path, sheet_name=sheet_name, engine=engine)
            logger.info(f"Successfully loaded Excel file: {file_path}")
            return True
        except Exception as e:
            self._add_error(f"Error loading Excel file: {e}")
            return False
    
    def _validate_sheet_not_empty(self):
        """Check if sheet has data"""
        if self.df.empty:
            self._add_error("Excel sheet is empty")
            return
            
        # Check if all values are null
        if self.df.notna().sum().sum() == 0:
            self._add_error("Excel sheet contains no data (all values are null)")
    
    def _validate_company_name(self):
        """Validate company name matches expected value"""
        expected_company = self.config.get("expected_company_name")
        if not expected_company:
            self._add_warning("No expected company name specified in configuration")
            return
            
        company_row_idx = None
        for idx, row in self.df.iterrows():
            if row.iloc[0] == "Name of the Company":
                company_row_idx = idx
                break
                
        if company_row_idx is None:
            self._add_error("Could not find 'Name of the Company' row")
            return
            
        # Check if company name cell has a value
        if pd.isna(self.df.iloc[company_row_idx, 1]):
            self._add_error("Company name cell is empty")
            return
        #print(self.df.iloc[company_row_idx])
        actual_company = str(self.df.iloc[company_row_idx, 1]).strip()
        if actual_company != expected_company:
            self._add_error(f"Company name mismatch. Expected: '{expected_company}', Found: '{actual_company}'")
    
    def _validate_has_audited_values(self):
        """Check if at least one 'Audited' value exists in specified row"""
        audit_row = self.config.get("Type_of_accounts_row")
        if not audit_row:
            self._add_warning("No audit type row specified in configuration")
            return
            
        try:
            audit_row_idx = audit_row - 1  # Convert to 0-based index
            audit_row_data = self.df.iloc[audit_row_idx]
            
            # Check if row exists
            if "Type of accounts" not in str(audit_row_data.iloc[0]):
                self._add_error(f"Row {audit_row} does not contain audit type information")
                return
                
            # Check for at least one 'Audited' value
            has_audited = False
            for i in range(1, len(audit_row_data)):
                if isinstance(audit_row_data.iloc[i], str) and "audit" in audit_row_data.iloc[i].lower():
                    has_audited = True
                    break
                    
            if not has_audited:
                self._add_error("No 'Audited' value found in audit type row")
                
        except Exception as e:
            self._add_error(f"Error validating audit types: {e}")
    
    def _validate_year_row(self):
        """Validate that the years row exists and contains valid years"""
        year_row = self.config.get("year_row")
        if not year_row:
            self._add_warning("No year row specified in configuration")
            return
            
        try:
            year_row_idx = year_row - 1  # Convert to 0-based index
            year_row_data = self.df.iloc[year_row_idx]
            
            # First cell should be empty or contain a specific header
            if not pd.isna(year_row_data.iloc[0]) and "year" not in str(year_row_data.iloc[0]).lower():
                self._add_warning(f"Year row first cell is not empty or does not contain 'year': {year_row_data.iloc[0]}")
            
            # Check that we have valid years
            valid_years = []
            for i in range(1, len(year_row_data)):
                cell_value = year_row_data.iloc[i]
                
                # Skip empty cells
                if pd.isna(cell_value):
                    continue
                
                # Try to convert to integer
                try:
                    if isinstance(cell_value, (int, float)):
                        year = int(cell_value)
                    else:
                        year = int(str(cell_value))
                        
                    # Basic validation - years should be reasonable
                    if 1900 <= year <= 2100:
                        valid_years.append(year)
                    else:
                        self._add_warning(f"Unusual year value in year row: {year}")
                except:
                    self._add_warning(f"Non-numeric year value in year row: {cell_value}")
            
            if not valid_years:
                self._add_error("No valid years found in the year row")
            else:
                logger.info(f"Found valid years: {valid_years}")
                
        except Exception as e:
            self._add_error(f"Error validating year row: {e}")
    
    def _validate_row_names(self):
        """Validate specified row names match expected values"""
        row_validations = self.config.get("row_validations", [])
        
        for validation in row_validations:
            row_num = validation.get("row")
            expected_name = validation.get("expected_name")
            
            if not row_num or not expected_name:
                continue
                
            try:
                row_idx = row_num - 1  # Convert to 0-based index
                actual_name = str(self.df.iloc[row_idx, 0])
                
                if expected_name != actual_name:
                    self._add_error(f"Row name mismatch at row {row_num}. Expected: '{expected_name}', Found: '{actual_name}'")
                    
            except Exception as e:
                self._add_error(f"Error validating row {row_num} name: {e}")
    
    def _validate_row_values(self):
        """Check if specified rows have at least one non-null value"""
        row_validations = self.config.get("row_validations", [])
        
        for validation in row_validations:
            row_num = validation.get("row")
            if not row_num:
                continue
                
            try:
                row_idx = row_num - 1  # Convert to 0-based index
                row_data = self.df.iloc[row_idx]
                
                # Skip first 1 columns (usually headers) and check for values
                has_value = False
                for i in range(1, len(row_data)):
                    if pd.notna(row_data.iloc[i]):
                        has_value = True
                        break
                        
                if not has_value:
                    row_name = str(row_data.iloc[0])
                    self._add_error(f"Row {row_num} ({row_name}) has no values across any year")
                    
            except Exception as e:
                self._add_error(f"Error validating values in row {row_num}: {e}")
    
    def _validate_column_continuity(self):
        """Validate that year columns are continuous with no gaps"""
        year_row = self.config.get("year_row")
        if not year_row:
            return
        
        try:
            year_row_idx = year_row - 1  # Convert to 0-based index
            year_row_data = self.df.iloc[year_row_idx]
            
            years = []
            for i in range(1, len(year_row_data)):
                cell_value = year_row_data.iloc[i]
                
                if pd.notna(cell_value):
                    try:
                        if isinstance(cell_value, (int, float)):
                            year = int(cell_value)
                        else:
                            year = int(str(cell_value))
                            
                        if 1900 <= year <= 2100:
                            years.append(year)
                    except:
                        pass
            
            # Check for gaps in years
            if years:
                years.sort()
                for i in range(len(years) - 1):
                    if years[i+1] - years[i] > 1:
                        self._add_warning(f"Gap in year sequence: {years[i]} to {years[i+1]}")
                        
                # Check minimum number of years if specified
                min_years = self.config.get("validation_settings", {}).get("min_required_years", 0)
                if min_years > 0 and len(years) < min_years:
                    self._add_error(f"Insufficient number of years. Found {len(years)}, required {min_years}")
                    
        except Exception as e:
            self._add_warning(f"Error checking year continuity: {e}")


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
    parser = argparse.ArgumentParser(description='Validate financial Excel data')
    parser.add_argument('--excel', required=True, help='Path to Excel file')
    parser.add_argument('--config', required=True, help='Path to validation configuration YAML file')
    parser.add_argument('--output', help='Path to output file for validation results (optional)')
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    if not config:
        logger.error("Failed to load configuration")
        return 1
    
    # Validate Excel file
    validator = ExcelValidator(config)
    results = validator.validate_file(args.excel)
    
    # Print validation summary
    print("\n=== VALIDATION SUMMARY ===")
    print(f"Valid: {'✓ YES' if results['is_valid'] else '✗ NO'}")
    
    if results['errors']:
        print("\nErrors:")
        for i, error in enumerate(results['errors'], 1):
            print(f"{i}. {error}")
    
    if results['warnings']:
        print("\nWarnings:")
        for i, warning in enumerate(results['warnings'], 1):
            print(f"{i}. {warning}")
    
    # Write results to file if specified
    if args.output:
        try:
            import json
            with open(args.output, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"\nResults written to {args.output}")
        except Exception as e:
            logger.error(f"Error writing results to file: {e}")
    
    return 0 if results['is_valid'] else 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)