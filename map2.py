#!/usr/bin/env python3
"""
Interactive Brokers Data Processing Script - IMPROVED VERSION
==============================================================

This script processes Interactive Brokers PDF reports and extracts financial metrics
into standardized CSV format. It uses multiple parsing strategies for maximum reliability.

Author: Interactive Brokers Data Processing Team
Version: 2.0 (Improved)
Date: 2025

Usage:
    python ibkr_parser.py                          # Normal processing
    python ibkr_parser.py debug <pdf_path>         # Debug PDF structure  
    python ibkr_parser.py test <pdf_path> <year> <month>  # Test extraction
"""

import os
import re
import csv
import sys
import pdfplumber
from pathlib import Path

# ==============================================================================
# 1. CONFIGURATION AND CONSTANTS
# ==============================================================================

class Config:
    """Configuration settings for the parser"""
    
    # Final CSV column order (standardized output format)
    FINAL_CSV_COLUMNS = [
        'USA.OBD.INTERACTIVE.ACCOUNTS.TOTAL.M',
        'USA.OBD.INTERACTIVE.ACCOUNTS.NETNEW.M',
        'USA.OBD.INTERACTIVE.DARTS.TOTAL.M', 
        'USA.OBD.INTERACTIVE.DARTS.CLEARED.M',
        'USA.OBD.INTERACTIVE.DARTS.CLEAREDAVG.M',
        'USA.OBD.INTERACTIVE.TRADING.OPTIONS.M',
        'USA.OBD.INTERACTIVE.TRADING.FUTURES.M',
        'USA.OBD.INTERACTIVE.TRADING.STOCKSHARES.M',
        'USA.OBD.INTERACTIVE.ACCOUNTLEVEL.EQUITY.M',
        'USA.OBD.INTERACTIVE.ACCOUNTLEVEL.FDIC.M',
        'USA.OBD.INTERACTIVE.ACCOUNTLEVEL.CREDITSATBROKER.M',
        'USA.OBD.INTERACTIVE.ACCOUNTLEVEL.CREDITSTOTAL.M',
        'USA.OBD.INTERACTIVE.ACCOUNTLEVEL.MARGINLOANS.M',
        'USA.OBD.INTERACTIVE.ACCOUNTLEVEL.CASH.M',
        'USA.OBD.INTERACTIVE.AVGCOMMISSION.STOCK.M',
        'USA.OBD.INTERACTIVE.AVGCOMMISSION.EQUITYOPTIONS.M',
        'USA.OBD.INTERACTIVE.AVGCOMMISSION.FUTURES.M',
        'USA.OBD.INTERACTIVE.AVGORDER.STOCK.M',
        'USA.OBD.INTERACTIVE.AVGORDER.EQUITYOPTIONS.M',
        'USA.OBD.INTERACTIVE.AVGORDER.FUTURES.M'
    ]
    
    # Descriptive headers for CSV output
    DESCRIPTIVE_HEADERS = {
        '': '',
        'USA.OBD.INTERACTIVE.ACCOUNTS.TOTAL.M': "From Monthly Metrics:\n Accounts:\n Total Accounts:\n Thousands",
        'USA.OBD.INTERACTIVE.ACCOUNTS.NETNEW.M': "From Monthly Metrics:\n Accounts:\n Net New Accounts:\n Thousands", 
        'USA.OBD.INTERACTIVE.DARTS.TOTAL.M': "From Monthly Metrics:\n DARTs:\n Total Client DARTs:\n Thousands",
        'USA.OBD.INTERACTIVE.DARTS.CLEARED.M': "From Monthly Metrics:\n DARTs:\n Cleared Client DARTs:\n Thousands",
        'USA.OBD.INTERACTIVE.DARTS.CLEAREDAVG.M': "From Monthly Metrics:\n DARTs:\n Cleared Avg. DART per Account:\n #",
        'USA.OBD.INTERACTIVE.TRADING.OPTIONS.M': "From Monthly Metrics:\n Trading Volumes:\n Options Contracts:\n Thousands",
        'USA.OBD.INTERACTIVE.TRADING.FUTURES.M': "From Monthly Metrics:\n Trading Volumes:\n Futures Contracts:\n Thousands",
        'USA.OBD.INTERACTIVE.TRADING.STOCKSHARES.M': "From Monthly Metrics:\n Trading Volumes:\n Stock Shares:\n Thousands",
        'USA.OBD.INTERACTIVE.ACCOUNTLEVEL.EQUITY.M': "From Monthly Metrics:\n Account Levels ($):\n Client Equity:\n $ Bln.",
        'USA.OBD.INTERACTIVE.ACCOUNTLEVEL.FDIC.M': "From Monthly Metrics:\n Account Levels ($):\n FDIC Program Client Credits:\n $ Bln.",
        'USA.OBD.INTERACTIVE.ACCOUNTLEVEL.CREDITSATBROKER.M': "From Monthly Metrics:\n Account Levels ($):\n Client Credits Held at Broker:\n $ Bln.",
        'USA.OBD.INTERACTIVE.ACCOUNTLEVEL.CREDITSTOTAL.M': "From Monthly Metrics:\n Account Levels ($):\n Client Credits (Total):\n $ Bln.",
        'USA.OBD.INTERACTIVE.ACCOUNTLEVEL.MARGINLOANS.M': "From Monthly Metrics:\n Account Levels ($):\n Client Margin Loans:\n $ Bln.",
        'USA.OBD.INTERACTIVE.ACCOUNTLEVEL.CASH.M': "From Monthly Metrics:\n Account Levels ($):\n Cash as % of Assets:\n %",
        'USA.OBD.INTERACTIVE.AVGCOMMISSION.STOCK.M': "From Press Release:\n Average Commission per Cleared Order:\n Stocks:\n $",
        'USA.OBD.INTERACTIVE.AVGCOMMISSION.EQUITYOPTIONS.M': "From Press Release:\n Average Commission per Cleared Order:\n Equity Options:\n $",
        'USA.OBD.INTERACTIVE.AVGCOMMISSION.FUTURES.M': "From Press Release:\n Average Commission per Cleared Order:\n Futures:\n $",
        'USA.OBD.INTERACTIVE.AVGORDER.STOCK.M': "From Press Release:\n Average Order Size:\n Stocks:\n # shares",
        'USA.OBD.INTERACTIVE.AVGORDER.EQUITYOPTIONS.M': "From Press Release:\n Average Order Size:\n Equity Options:\n # contracts",
        'USA.OBD.INTERACTIVE.AVGORDER.FUTURES.M': "From Press Release:\n Average Order Size:\n Futures:\n # contracts",
    }
    
    # Data source mapping
    MAPPING_CONFIG = {
        'USA.OBD.INTERACTIVE.ACCOUNTS.TOTAL.M': {'source_name': 'Total Accounts', 'source': 'monthly_brokerage'},
        'USA.OBD.INTERACTIVE.ACCOUNTS.NETNEW.M': {'source_name': 'Net New Accounts', 'source': 'monthly_brokerage'},
        'USA.OBD.INTERACTIVE.DARTS.TOTAL.M': {'source_name': 'Total Client DARTs', 'source': 'monthly_brokerage'},
        'USA.OBD.INTERACTIVE.DARTS.CLEARED.M': {'source_name': 'Cleared Client DARTs', 'source': 'monthly_brokerage'},
        'USA.OBD.INTERACTIVE.DARTS.CLEAREDAVG.M': {'source_name': 'Cleared Avg. DART per Account (Annualized)', 'source': 'monthly_brokerage'},
        'USA.OBD.INTERACTIVE.TRADING.OPTIONS.M': {'source_name': 'Options Contracts', 'source': 'monthly_brokerage'},
        'USA.OBD.INTERACTIVE.TRADING.FUTURES.M': {'source_name': 'Futures Contracts', 'source': 'monthly_brokerage'},
        'USA.OBD.INTERACTIVE.TRADING.STOCKSHARES.M': {'source_name': 'Stock Shares', 'source': 'monthly_brokerage'},
        'USA.OBD.INTERACTIVE.ACCOUNTLEVEL.EQUITY.M': {'source_name': 'Client Equity', 'source': 'monthly_brokerage'},
        'USA.OBD.INTERACTIVE.ACCOUNTLEVEL.FDIC.M': {'source_name': 'FDIC Program Client Credits', 'source': 'monthly_brokerage'},
        'USA.OBD.INTERACTIVE.ACCOUNTLEVEL.CREDITSATBROKER.M': {'source_name': 'Client Credits Held at Broker', 'source': 'monthly_brokerage'},
        'USA.OBD.INTERACTIVE.ACCOUNTLEVEL.CREDITSTOTAL.M': {'source_name': 'Client Credits', 'source': 'monthly_brokerage'},
        'USA.OBD.INTERACTIVE.ACCOUNTLEVEL.MARGINLOANS.M': {'source_name': 'Client Margin Loans', 'source': 'monthly_brokerage'},
        'USA.OBD.INTERACTIVE.ACCOUNTLEVEL.CASH.M': {'source_name': 'Cash as % of Assets', 'source': 'monthly_brokerage'},
        'USA.OBD.INTERACTIVE.AVGCOMMISSION.STOCK.M': {'source_name': ('Stocks', 'Average Commission'), 'source': 'press_release'},
        'USA.OBD.INTERACTIVE.AVGCOMMISSION.EQUITYOPTIONS.M': {'source_name': ('Equity Options', 'Average Commission'), 'source': 'press_release'},
        'USA.OBD.INTERACTIVE.AVGCOMMISSION.FUTURES.M': {'source_name': ('Futures', 'Average Commission'), 'source': 'press_release'},
        'USA.OBD.INTERACTIVE.AVGORDER.STOCK.M': {'source_name': ('Stocks', 'Average Order Size'), 'source': 'press_release'},
        'USA.OBD.INTERACTIVE.AVGORDER.EQUITYOPTIONS.M': {'source_name': ('Equity Options', 'Average Order Size'), 'source': 'press_release'},
        'USA.OBD.INTERACTIVE.AVGORDER.FUTURES.M': {'source_name': ('Futures', 'Average Order Size'), 'source': 'press_release'},
    }
    
    # Month mapping
    MONTH_MAPPING = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
        7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }
    
    VALID_MONTHS = list(MONTH_MAPPING.values())

# ==============================================================================
# 2. UTILITY CLASSES AND FUNCTIONS  
# ==============================================================================

class Logger:
    """Simple logging utility"""

    @staticmethod
    def info(message):
        print(f"INFO: {message}")

    @staticmethod
    def success(message):
        print(f"SUCCESS: {message}")

    @staticmethod
    def warning(message):
        print(f"WARNING: {message}")

    @staticmethod
    def error(message):
        print(f"ERROR: {message}")

    @staticmethod
    def debug(message):
        print(f"DEBUG: {message}")

def extract_pdf_from_java_wrapper(file_path):
    """Extract actual PDF content from Java-serialized wrapper if present"""
    try:
        with open(file_path, 'rb') as f:
            data = f.read()

        # Check if this is a Java-wrapped PDF
        if data.startswith(b'\xac\xed'):  # Java serialization magic bytes
            # Find the PDF header
            pdf_start = data.find(b'%PDF-')
            if pdf_start == -1:
                return file_path  # No PDF content found, return original

            # Find the PDF end marker
            pdf_end = data.rfind(b'%%EOF')
            if pdf_end == -1:
                return file_path  # No PDF end marker, return original

            # Extract PDF content
            pdf_content = data[pdf_start:pdf_end + 5]

            # Create temporary clean PDF file
            import tempfile
            temp_fd, temp_path = tempfile.mkstemp(suffix='.pdf')
            os.close(temp_fd)

            with open(temp_path, 'wb') as f:
                f.write(pdf_content)

            Logger.debug(f"Extracted PDF from Java wrapper: {len(pdf_content)} bytes")
            return temp_path

        # Not a Java wrapper, return original path
        return file_path

    except Exception as e:
        Logger.warning(f"Could not check for Java wrapper: {e}")
        return file_path

def clean_numeric_value(value_str):
    """Clean and normalize numeric values"""
    if not value_str:
        return ""
    return re.sub(r'[$,]', '', str(value_str)).strip()

def validate_date_params(year, month_num):
    """Validate year and month parameters"""
    try:
        year_int = int(year)
        month_int = int(month_num)

        if not (2015 <= year_int <= 2030):
            return False, "Year must be between 2015 and 2030"

        if not (1 <= month_int <= 12):
            return False, "Month must be between 1 and 12"

        return True, None
    except (ValueError, TypeError):
        return False, "Year and month must be valid integers"

def extract_date_from_content(text, logger=None):
    """Extract year and month from PDF content as fallback"""
    if logger is None:
        logger = Logger()

    month_name_to_num = {
        'january': 1, 'february': 2, 'march': 3, 'april': 4,
        'may': 5, 'june': 6, 'july': 7, 'august': 8,
        'september': 9, 'october': 10, 'november': 11, 'december': 12,
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
        'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
        'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }

    # Patterns for extracting dates from content
    patterns = [
        # Monthly Brokerage patterns
        r'(\d{4})\s*%?\s*Change',                    # "2025 % Change"
        r'ELECTRONIC\s+BROKERAGE.*?(\d{4})',         # Header with year

        # Press Release patterns
        r'for\s+(\w+)\s+(\d{4})',                    # "for August 2025"
        r'(\w+)\s+(\d{4}),?\s*includes',             # "August 2025, includes"
        r'(\w+)\s+\d+,\s+(\d{4})',                  # "September 2, 2025"
        r'metrics\s+for\s+(\w+)',                   # "metrics for August"
        r'performance\s+metrics\s+for\s+(\w+)',     # "performance metrics for August"

        # Generic patterns
        r'(\w+)\s+(\d{4})'                          # "Month Year" format
    ]

    text_lower = text.lower()

    for pattern in patterns:
        matches = re.finditer(pattern, text_lower, re.IGNORECASE)
        for match in matches:
            groups = match.groups()

            if len(groups) == 1:
                # Single group - could be year
                if groups[0].isdigit() and len(groups[0]) == 4:
                    year = int(groups[0])
                    # Need to find month separately for this case
                    continue

            elif len(groups) == 2:
                # Two groups - could be month+year
                group1, group2 = groups

                if group1.isdigit() and len(group1) == 4:
                    # First group is year, second might be month
                    year = int(group1)
                    if group2.lower() in month_name_to_num:
                        month = month_name_to_num[group2.lower()]
                        logger.debug(f"Extracted date from content: {year}-{month:02d} using pattern '{pattern}'")
                        return year, month

                elif group2.isdigit() and len(group2) == 4:
                    # Second group is year, first might be month
                    year = int(group2)
                    if group1.lower() in month_name_to_num:
                        month = month_name_to_num[group1.lower()]
                        logger.debug(f"Extracted date from content: {year}-{month:02d} using pattern '{pattern}'")
                        return year, month

    logger.debug("Could not extract date from PDF content")
    return None, None

def detect_latest_month_from_data(text, year, logger=None):
    """Detect the latest month with data in Monthly Brokerage PDF"""
    if logger is None:
        logger = Logger()

    lines = text.split('\n')

    # Find the month header line
    month_header_line = None
    for line in lines:
        if all(month in line for month in ['Jan', 'Feb', 'Mar']) and 'Aug' in line:
            month_header_line = line
            break

    if not month_header_line:
        return None

    # Find data lines and check which months have data
    months_with_data = []
    month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

    for line in lines:
        # Skip header and non-data lines
        if any(month in line for month in month_order[:3]):
            continue
        if not any(char.isdigit() for char in line):
            continue

        # Count numeric values in the line
        numeric_parts = re.findall(r'[\d,]+\.?\d*', line)
        if len(numeric_parts) >= 8:  # If we have at least 8 months of data
            months_with_data = month_order[:len(numeric_parts)]
            break

    if months_with_data:
        latest_month = months_with_data[-1]
        month_num = month_order.index(latest_month) + 1
        logger.debug(f"Detected latest month with data: {latest_month} ({month_num})")
        return month_num

    return None

# ==============================================================================
# 3. PDF PARSING ENGINE
# ==============================================================================

class PDFParser:
    """Main PDF parsing engine with multiple strategies"""
    
    def __init__(self):
        self.logger = Logger()
    
    def parse_monthly_brokerage_data(self, pdf_path, target_year, target_month_num):
        """
        Main entry point for monthly brokerage data parsing.
        Uses multiple strategies in order of reliability.
        """
        target_month_abbr = Config.MONTH_MAPPING.get(target_month_num)
        if not target_month_abbr:
            return {"Error": f"Invalid target month number: {target_month_num}"}

        # Validate inputs
        valid, error_msg = validate_date_params(target_year, target_month_num)
        if not valid:
            return {"Error": error_msg}

        self.logger.info(f"Parsing {Path(pdf_path).name} for {target_year} {target_month_abbr}")

        # Extract from Java wrapper if needed
        clean_pdf_path = extract_pdf_from_java_wrapper(pdf_path)
        temp_file_created = clean_pdf_path != pdf_path

        try:
            with pdfplumber.open(clean_pdf_path) as pdf:
                if not pdf.pages:
                    return {"Error": "PDF has no pages"}
                
                # Strategy 1: Text-based extraction (most reliable)
                self.logger.info("Trying text-based extraction...")
                result = self._parse_using_text_extraction(pdf, target_year, target_month_abbr)
                
                if result and "Error" not in result and len(result) >= 5:
                    self.logger.success(f"Text extraction successful: {len(result)} metrics found")
                    return result
                
                # Strategy 2: Coordinate-based extraction
                self.logger.warning("Text extraction insufficient, trying coordinate-based...")
                result = self._parse_using_coordinates(pdf, target_year, target_month_abbr)
                
                if result and "Error" not in result and len(result) >= 5:
                    self.logger.success(f"Coordinate extraction successful: {len(result)} metrics found")
                    return result
                
                # Strategy 3: Table extraction
                self.logger.warning("Coordinate extraction insufficient, trying table extraction...")
                result = self._parse_using_table_extraction(pdf, target_year, target_month_abbr)
                
                if result and "Error" not in result:
                    self.logger.success(f"Table extraction successful: {len(result)} metrics found")
                    return result
                
                return {"Error": "All parsing strategies failed to extract sufficient data"}

        except Exception as e:
            return {"Error": f"PDF processing failed: {str(e)}"}
        finally:
            # Clean up temporary file if created
            if temp_file_created and os.path.exists(clean_pdf_path):
                try:
                    os.unlink(clean_pdf_path)
                except:
                    pass
    
    def _parse_using_text_extraction(self, pdf, target_year, target_month_abbr):
        """Strategy 1: Text extraction with regex patterns"""
        try:
            # Extract all text from PDF
            full_text = ""
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    full_text += page_text + "\n"
            
            if not full_text.strip():
                return {"Error": "No text extracted from PDF"}
            
            # Check if target year exists
            if str(target_year) not in full_text:
                return {"Error": f"Target year {target_year} not found in PDF"}
            
            # Split into lines and find header
            lines = full_text.split('\n')
            header_line_idx = -1
            
            for i, line in enumerate(lines):
                if all(month in line for month in ['Jan', 'Feb', 'Mar']) and target_month_abbr in line:
                    header_line_idx = i
                    break
            
            if header_line_idx == -1:
                return {"Error": f"Could not find header line with {target_month_abbr}"}
            
            # Determine target month column index with improved logic
            header_line = lines[header_line_idx]
            self.logger.debug(f"Header line: {header_line}")

            # Split header by whitespace to get proper column alignment
            header_parts = header_line.split()
            month_positions = []

            # Find month positions in header to validate target month exists
            for i, part in enumerate(header_parts):
                for month in Config.VALID_MONTHS:
                    if month in part:
                        month_positions.append((month, i))
                        break

            self.logger.debug(f"Found months in header: {month_positions}")

            # Verify target month exists in header
            target_month_found = any(month == target_month_abbr for month, _ in month_positions)
            if not target_month_found:
                return {"Error": f"Could not locate {target_month_abbr} in header"}

            # Map target month to data column index (0-based)
            # Assumes data columns are in chronological order: Jan=0, Feb=1, ..., Dec=11
            month_to_index = {
                'Jan': 0, 'Feb': 1, 'Mar': 2, 'Apr': 3, 'May': 4, 'Jun': 5,
                'Jul': 6, 'Aug': 7, 'Sep': 8, 'Oct': 9, 'Nov': 10, 'Dec': 11
            }

            target_month_idx = month_to_index.get(target_month_abbr, -1)
            if target_month_idx == -1:
                return {"Error": f"Unsupported month: {target_month_abbr}"}

            self.logger.debug(f"Target month {target_month_abbr} mapped to data column index: {target_month_idx}")
            
            # Define metric patterns with improved robustness
            metric_patterns = {
                'Total Accounts': r'Total Accounts[^\d]*(\d+(?:,\d+)*\.?\d*)',
                'Net New Accounts': r'Net New Accounts[^\d]*(\d+(?:,\d+)*\.?\d*)',
                'Total Client DARTs': r'Total Client DARTs[^\d]*(\d+(?:,\d+)*)',
                'Cleared Client DARTs': r'Cleared Client DARTs[^\d]*(\d+(?:,\d+)*)', 
                'Options Contracts': r'Options Contracts[^\d]*(\d+(?:,\d+)*)',
                'Futures Contracts': r'Futures Contracts[^\d]*(\d+(?:,\d+)*)',
                'Stock Shares': r'Stock Shares[^\d]*(\d+(?:,\d+)*)',
                'Client Equity': r'Client Equity[^\d]*\$?(\d+(?:,\d+)*\.?\d*)',
                'FDIC Program Client Credits': r'FDIC Program Client Credits[^\d]*\$?(\d+(?:,\d+)*\.?\d*)',
                'Client Credits Held at Broker': r'Client Credits Held at Broker[^\d]*\$?(\d+(?:,\d+)*\.?\d*)',
                'Client Credits': r'Client Credits\(\d+\)[^\d]*\$?(\d+(?:,\d+)*\.?\d*)',
                'Client Margin Loans': r'Client Margin Loans[^\d]*\$?(\d+(?:,\d+)*\.?\d*)',
                'Cash as % of Assets': r'Cash as % of Assets[^\d]*(\d+(?:\.?\d*))%?',
            }
            
            extracted_data = {}
            
            # Process each line after header
            for line_idx in range(header_line_idx + 1, min(header_line_idx + 25, len(lines))):
                if line_idx >= len(lines):
                    break
                    
                line = lines[line_idx].strip()
                if not line:
                    continue
                
                # Handle multi-line metrics (combine with next line if needed)
                combined_line = line
                if line_idx + 1 < len(lines):
                    next_line = lines[line_idx + 1].strip()
                    if next_line and ("Annualized" in next_line or len(line.split()) < 3):
                        combined_line = line + " " + next_line
                
                # Test each metric pattern
                for metric_name, pattern in metric_patterns.items():
                    if metric_name in extracted_data:  # Skip if already found
                        continue

                    if re.search(pattern, combined_line, re.IGNORECASE):
                        # Split line by whitespace to get proper alignment with header
                        line_parts = combined_line.split()

                        # Find numeric parts only
                        numeric_parts = []
                        for part in line_parts:
                            # Check if part is numeric (with commas, decimals, dollar signs)
                            if re.match(r'^[\$]?[\d,]+\.?\d*$', part):
                                numeric_parts.append(part.replace('$', '').replace(',', ''))

                        self.logger.debug(f"Line: {combined_line}")
                        self.logger.debug(f"Numeric parts: {numeric_parts}")
                        self.logger.debug(f"Target index: {target_month_idx}, Available: {len(numeric_parts)}")

                        if numeric_parts and len(numeric_parts) > target_month_idx:
                            value = clean_numeric_value(numeric_parts[target_month_idx])
                            if value:
                                extracted_data[metric_name] = value
                                self.logger.debug(f"Found {metric_name}: {value} (from index {target_month_idx})")
                
                # Special handling for "Cleared Avg. DART per Account (Annualized)"
                if ("Cleared Avg. DART per Account" in line or "Annualized" in line) and 'Cleared Avg. DART per Account (Annualized)' not in extracted_data:
                    line_parts = combined_line.split()
                    numeric_parts = []
                    for part in line_parts:
                        if re.match(r'^[\$]?[\d,]+\.?\d*$', part):
                            numeric_parts.append(part.replace('$', '').replace(',', ''))

                    if numeric_parts and len(numeric_parts) > target_month_idx:
                        value = clean_numeric_value(numeric_parts[target_month_idx])
                        if value:
                            extracted_data['Cleared Avg. DART per Account (Annualized)'] = value
                            self.logger.debug(f"Found DART per Account: {value} (from index {target_month_idx})")
            
            return extracted_data
            
        except Exception as e:
            return {"Error": f"Text extraction failed: {str(e)}"}
    
    def _parse_using_coordinates(self, pdf, target_year, target_month_abbr):
        """Strategy 2: Coordinate-based extraction (fallback)"""
        try:
            page = pdf.pages[0]
            words = page.extract_words(x_tolerance=3, y_tolerance=3)
            
            if not words:
                return {"Error": "No words extracted from PDF"}
            
            # Find year header word
            year_header_word = None
            for w in words:
                if str(target_year) in w["text"]:
                    year_header_word = w
                    break
            
            if not year_header_word:
                return {"Error": f"Year {target_year} not found in word coordinates"}
            
            # Find month headers
            month_headers = []
            for w in words:
                if w["text"] in Config.VALID_MONTHS and w["top"] > year_header_word["top"] - 15:
                    month_headers.append(w)
            
            month_headers.sort(key=lambda w: w["x0"])
            
            if len(month_headers) < 3:
                return {"Error": f"Insufficient month headers found: {len(month_headers)}"}
            
            # Find target month column boundaries
            target_col_start, target_col_end = -1, float('inf')
            for i, header in enumerate(month_headers):
                if header["text"] == target_month_abbr:
                    target_col_start = header["x0"] - 8  # Increased tolerance
                    if i + 1 < len(month_headers):
                        target_col_end = month_headers[i+1]["x0"] - 8
                    break
            
            if target_col_start == -1:
                return {"Error": f"Could not find column boundaries for {target_month_abbr}"}
            
            # Extract data using coordinates
            extracted_data = {}
            numeric_pattern = re.compile(r'^[$\d,.]+$')
            metric_source_names = [v['source_name'] for v in Config.MAPPING_CONFIG.values() 
                                 if v.get('source') == 'monthly_brokerage']
            
            # Group words by row (Y position)  
            rows = {}
            for w in words:
                row_key = round(w["top"] / 3) * 3
                if row_key not in rows:
                    rows[row_key] = []
                rows[row_key].append(w)
            
            # Process each row
            for y_pos, row_words in rows.items():
                row_text = " ".join([w["text"] for w in row_words]).lower()
                
                for metric_name in metric_source_names:
                    metric_words = metric_name.lower().split()
                    if any(word in row_text for word in metric_words if len(word) > 3):
                        # Find numeric values in target column
                        numeric_words = [w for w in row_words 
                                       if numeric_pattern.match(w["text"]) and 
                                       target_col_start <= w["x0"] < target_col_end]
                        
                        if numeric_words:
                            value = clean_numeric_value(numeric_words[0]["text"])
                            if value:
                                extracted_data[metric_name] = value
                                self.logger.debug(f"Coordinate: {metric_name} = {value}")
            
            return extracted_data
            
        except Exception as e:
            return {"Error": f"Coordinate extraction failed: {str(e)}"}
    
    def _parse_using_table_extraction(self, pdf, target_year, target_month_abbr):
        """Strategy 3: Table-based extraction (last resort)"""
        try:
            page = pdf.pages[0]
            tables = page.extract_tables()
            
            if not tables:
                return {"Error": "No tables found in PDF"}
            
            extracted_data = {}
            
            for table_idx, table in enumerate(tables):
                if not table or len(table) < 2:
                    continue
                
                # Find header row and target column
                header_row_idx = -1
                target_month_col = -1
                
                for row_idx, row in enumerate(table):
                    if row and any(month in str(cell) for cell in row if cell for month in Config.VALID_MONTHS):
                        header_row_idx = row_idx
                        for col_idx, cell in enumerate(row):
                            if cell and target_month_abbr in str(cell):
                                target_month_col = col_idx
                                break
                        break
                
                if header_row_idx == -1 or target_month_col == -1:
                    continue
                
                # Extract data rows
                metric_source_names = [v['source_name'] for v in Config.MAPPING_CONFIG.values() 
                                     if v.get('source') == 'monthly_brokerage']
                
                for row_idx in range(header_row_idx + 1, len(table)):
                    row = table[row_idx]
                    if not row or len(row) <= target_month_col:
                        continue
                    
                    metric_cell = str(row[0]) if row[0] else ""
                    value_cell = str(row[target_month_col]) if len(row) > target_month_col and row[target_month_col] else ""
                    
                    if not metric_cell or not value_cell:
                        continue
                    
                    # Match against known metrics
                    for metric_name in metric_source_names:
                        if any(word.lower() in metric_cell.lower() for word in metric_name.split() if len(word) > 3):
                            clean_value = clean_numeric_value(value_cell)
                            if clean_value and re.match(r'^\d+\.?\d*$', clean_value):
                                extracted_data[metric_name] = clean_value
                                self.logger.debug(f"Table: {metric_name} = {clean_value}")
            
            return extracted_data
            
        except Exception as e:
            return {"Error": f"Table extraction failed: {str(e)}"}
    
    def parse_press_release(self, pdf_path):
        """Parse press release PDF with improved pattern matching"""
        # Extract from Java wrapper if needed
        clean_pdf_path = extract_pdf_from_java_wrapper(pdf_path)
        temp_file_created = clean_pdf_path != pdf_path

        try:
            with pdfplumber.open(clean_pdf_path) as pdf:
                report_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

            if not report_text.strip():
                return {"Error": "No text extracted from press release PDF"}
            
            data = {}
            
            # Primary patterns (narrative format)
            primary_patterns = {
                'Stocks': r"Stocks\s+([\d,]+\s+shares)\s+\$([\d.]+)",
                'Equity Options': r"Equity\s+Options\s+([\d.]+\s+contracts)\s+\$([\d.]+)",
                'Futures': r"Futures\s+and\s+Future\s+Options\s+([\d.]+\s+contracts)\s+\$([\d.]+)"
            }
            
            # Backup patterns (table format)
            backup_patterns = {
                'Stocks': r"Stocks\s+(\d+(?:,\d+)*)\s+shares\s+\$(\d+\.?\d*)",
                'Equity Options': r"Equity\s+Options\s+(\d+\.?\d*)\s+contracts\s+\$(\d+\.?\d*)",
                'Futures': r"Futures\s+(\d+\.?\d*)\s+contracts\s+\$(\d+\.?\d*)"
            }
            
            # Try primary patterns first, then backup
            for pattern_name, patterns_dict in [("primary", primary_patterns), ("backup", backup_patterns)]:
                for product, pattern in patterns_dict.items():
                    if product not in data:  # Only if not already found
                        match = re.search(pattern, report_text, re.MULTILINE | re.IGNORECASE)
                        if match:
                            self.logger.debug(f"Found {product} using {pattern_name} pattern")
                            
                            if pattern_name == "backup":
                                # Backup pattern: numbers only
                                order_size = match.group(1).replace(',', '')
                                commission = match.group(2)
                            else:
                                # Primary pattern: with units
                                order_size = match.group(1).replace(" shares", "").replace(" contracts", "").replace(',', '').strip()
                                commission = match.group(2).strip()
                            
                            data[product] = {
                                'Average Order Size': order_size,
                                'Average Commission': commission
                            }
            
            if not data:
                self.logger.warning("No commission/order size data extracted from press release")
            else:
                self.logger.success(f"Press release extraction successful: {len(data)} products found")
            
            return data

        except Exception as e:
            return {"Error": f"Press release parsing failed: {str(e)}"}
        finally:
            # Clean up temporary file if created
            if temp_file_created and os.path.exists(clean_pdf_path):
                try:
                    os.unlink(clean_pdf_path)
                except:
                    pass

# ==============================================================================
# 4. DATA PROCESSING AND OUTPUT
# ==============================================================================

class DataProcessor:
    """Handles data processing and CSV output generation"""
    
    def __init__(self):
        self.logger = Logger()
    
    def create_csv_output(self, final_data, date_str, filename):
        """Create standardized CSV output"""
        try:
            header_row1 = ['Date'] + Config.FINAL_CSV_COLUMNS
            header_row2 = [''] + [Config.DESCRIPTIVE_HEADERS.get(h, "") for h in Config.FINAL_CSV_COLUMNS]
            data_row = [date_str] + [final_data.get(h, "") for h in Config.FINAL_CSV_COLUMNS]
            
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(header_row1)
                writer.writerow(header_row2)
                writer.writerow(data_row)
            
            self.logger.success(f"Created CSV file: {filename}")
            return True
            
        except IOError as e:
            self.logger.error(f"Could not write CSV file {filename}: {e}")
            return False
    
    def process_pdf_pair(self, brokerage_pdf_path, press_release_pdf_path, date_prefix):
        """Process a complete pair of PDF reports with fallback date detection"""
        # Primary: Extract from filename
        target_year = None
        target_month_num = None

        if date_prefix and len(date_prefix) == 6 and date_prefix.isdigit():
            target_year = date_prefix[:4]
            target_month_num = int(date_prefix[4:])
            self.logger.info(f"Using date from filename: {target_year}-{target_month_num:02d}")
        else:
            self.logger.info("Filename date not available or invalid, using fallback detection")

        # Fallback: Extract from PDF content
        if not target_year or not target_month_num:
            self.logger.info("Attempting fallback date extraction from PDF content...")

            # Try Monthly Brokerage PDF first
            try:
                import pdfplumber
                with pdfplumber.open(brokerage_pdf_path) as pdf:
                    brokerage_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

                # Extract year from brokerage content
                fallback_year, fallback_month = extract_date_from_content(brokerage_text, self.logger)

                if fallback_year:
                    target_year = str(fallback_year)
                    self.logger.info(f"Extracted year from Monthly Brokerage PDF: {target_year}")

                    # Detect latest month with data if month not found
                    if not fallback_month:
                        detected_month = detect_latest_month_from_data(brokerage_text, fallback_year, self.logger)
                        if detected_month:
                            target_month_num = detected_month
                    else:
                        target_month_num = fallback_month

            except Exception as e:
                self.logger.warning(f"Could not extract date from Monthly Brokerage PDF: {e}")

            # Try Press Release PDF if still no date
            if not target_year or not target_month_num:
                try:
                    with pdfplumber.open(press_release_pdf_path) as pdf:
                        press_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

                    fallback_year, fallback_month = extract_date_from_content(press_text, self.logger)

                    if fallback_year and not target_year:
                        target_year = str(fallback_year)
                        self.logger.info(f"Extracted year from Press Release PDF: {target_year}")

                    if fallback_month and not target_month_num:
                        target_month_num = fallback_month
                        self.logger.info(f"Extracted month from Press Release PDF: {target_month_num}")

                except Exception as e:
                    self.logger.warning(f"Could not extract date from Press Release PDF: {e}")

        # Validate final date
        if not target_year or not target_month_num:
            self.logger.error("Could not determine target year and month from filename or PDF content")
            return False

        # Convert to proper types
        target_year = str(target_year)
        target_month_num = int(target_month_num)

        self.logger.info("="*80)
        self.logger.info(f"PROCESSING: {target_year}-{target_month_num:02d}")
        self.logger.info(f"Brokerage PDF: {Path(brokerage_pdf_path).name}")
        self.logger.info(f"Press Release PDF: {Path(press_release_pdf_path).name}")
        self.logger.info("="*80)
        
        # Initialize parser
        parser = PDFParser()
        
        # Parse both PDFs
        brokerage_data = parser.parse_monthly_brokerage_data(brokerage_pdf_path, target_year, target_month_num)
        press_release_data = parser.parse_press_release(press_release_pdf_path)
        
        # Check for critical errors
        if "Error" in brokerage_data:
            self.logger.error(f"Brokerage parsing failed: {brokerage_data['Error']}")
            return False
        
        if "Error" in press_release_data:
            self.logger.warning(f"Press release parsing failed: {press_release_data['Error']}")
            # Continue processing - press release is not critical
            press_release_data = {}
        
        # Map extracted data to final CSV format
        final_mapped_data = {}
        missing_data = []

        for csv_header in Config.FINAL_CSV_COLUMNS:
            config = Config.MAPPING_CONFIG.get(csv_header, {})
            value = ""

            if config:
                source_name = config['source_name']
                if config['source'] == 'monthly_brokerage':
                    value = brokerage_data.get(source_name, "")
                    if not value:
                        missing_data.append(f"Monthly: {source_name}")
                elif config['source'] == 'press_release':
                    if isinstance(source_name, tuple):
                        product, metric_type = source_name
                        value = press_release_data.get(product, {}).get(metric_type, "")
                        if not value:
                            missing_data.append(f"Press Release: {product} {metric_type}")

            final_mapped_data[csv_header] = value

        # Calculate Cash as % of Assets: (Client Credits Total / Client Equity) * 100
        credits_total = final_mapped_data.get('USA.OBD.INTERACTIVE.ACCOUNTLEVEL.CREDITSTOTAL.M', '')
        client_equity = final_mapped_data.get('USA.OBD.INTERACTIVE.ACCOUNTLEVEL.EQUITY.M', '')

        if credits_total and client_equity:
            try:
                credits_val = float(str(credits_total).replace(',', ''))
                equity_val = float(str(client_equity).replace(',', ''))
                if equity_val > 0:
                    cash_percentage = (credits_val / equity_val) * 100
                    final_mapped_data['USA.OBD.INTERACTIVE.ACCOUNTLEVEL.CASH.M'] = f"{cash_percentage:.11f}"
                    self.logger.info(f"Calculated Cash %: {credits_val}/{equity_val}*100 = {cash_percentage:.11f}")
            except (ValueError, ZeroDivisionError) as e:
                self.logger.warning(f"Could not calculate Cash % of Assets: {e}")
        
        # Report results
        total_possible = len(Config.FINAL_CSV_COLUMNS)
        total_extracted = sum(1 for v in final_mapped_data.values() if v)
        
        self.logger.info(f"Data extraction summary:")
        self.logger.info(f"  Brokerage metrics: {len(brokerage_data)} extracted")
        self.logger.info(f"  Press release metrics: {len(press_release_data)} extracted")
        self.logger.info(f"  Total fields populated: {total_extracted}/{total_possible}")
        
        if missing_data:
            self.logger.warning(f"Missing data for: {', '.join(missing_data[:5])}{'...' if len(missing_data) > 5 else ''}")
        
        # Generate CSV output
        date_str_for_csv = f"{target_year}-{target_month_num:02d}"
        output_filename = f"IBKR_DATA_OUTPUT_{date_prefix}.csv"
        
        success = self.create_csv_output(final_mapped_data, date_str_for_csv, output_filename)
        
        if success:
            self.logger.success(f"Processing completed successfully for {date_prefix}")
        else:
            self.logger.error(f"Failed to create output file for {date_prefix}")
        
        return success

# ==============================================================================
# 5. FILE DISCOVERY AND BATCH PROCESSING
# ==============================================================================

class FileManager:
    """Handles PDF file discovery and batch processing"""
    
    def __init__(self):
        self.logger = Logger()
        self.processor = DataProcessor()
    
    def find_and_process_all_reports(self):
        """Scan for PDF pairs and process them"""
        self.logger.info("Scanning for PDF report pairs...")
        
        script_dir = Path(__file__).parent
        processed_files = set()
        successful_processing = 0
        
        # Walk through directories
        for root in [script_dir] + list(script_dir.rglob("*")):
            if not root.is_dir():
                continue
                
            self.logger.debug(f"Checking directory: {root}")
            report_groups = {}
            
            # Find PDF files with correct naming pattern
            for file_path in root.glob("*.pdf"):
                match = re.match(r"(\d{6})(MetricsPressRelease|MonthlyBrokerageData)\.pdf", 
                               file_path.name, re.IGNORECASE)
                if match:
                    date_prefix, report_type = match.groups()
                    self.logger.debug(f"Found: {file_path.name} -> {date_prefix} {report_type}")
                    
                    if date_prefix not in report_groups:
                        report_groups[date_prefix] = {}
                    report_groups[date_prefix][report_type] = file_path
            
            # Process complete pairs
            for date_prefix, paths in report_groups.items():
                required_types = ['MetricsPressRelease', 'MonthlyBrokerageData']
                
                if all(report_type in paths for report_type in required_types):
                    file_pair_key = tuple(sorted(str(p) for p in paths.values()))
                    
                    if file_pair_key in processed_files:
                        continue
                    
                    self.logger.info(f"Found complete pair for {date_prefix}")
                    
                    success = self.processor.process_pdf_pair(
                        paths['MonthlyBrokerageData'], 
                        paths['MetricsPressRelease'], 
                        date_prefix
                    )
                    
                    if success:
                        successful_processing += 1
                    
                    processed_files.add(file_pair_key)
                else:
                    missing = [t for t in required_types if t not in paths]
                    self.logger.warning(f"Incomplete pair for {date_prefix}, missing: {missing}")
        
        # Final summary
        if not processed_files:
            self.logger.error("No complete report pairs found!")
            self.logger.info("Ensure files are named: YYYYMMMonthlyBrokerageData.pdf and YYYYMMMetricsPressRelease.pdf")
        else:
            self.logger.success(f"Processing complete: {successful_processing}/{len(processed_files)} successful")
        
        return len(processed_files), successful_processing

# ==============================================================================
# 6. DEBUGGING AND DIAGNOSTIC TOOLS
# ==============================================================================

class DebugTools:
    """Diagnostic and debugging utilities"""
    
    def __init__(self):
        self.logger = Logger()
    
    def analyze_pdf_structure(self, pdf_path):
        """Analyze PDF structure for troubleshooting"""
        self.logger.info(f"🔍 ANALYZING PDF STRUCTURE: {Path(pdf_path).name}")
        self.logger.info("="*80)
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                self.logger.info(f"📖 Total pages: {len(pdf.pages)}")
                
                for page_num, page in enumerate(pdf.pages[:2]):  # First 2 pages only
                    self.logger.info(f"\n--- Page {page_num + 1} ---")
                    self.logger.info(f"Dimensions: {page.width:.1f} x {page.height:.1f}")
                    
                    # Text extraction
                    text = page.extract_text()
                    if text:
                        self.logger.info(f"Text characters: {len(text)}")
                        self.logger.info("First 200 characters:")
                        print(repr(text[:200]))
                        
                        # Look for key indicators
                        indicators = ['Total Accounts', 'Client DARTs', 'Options Contracts', 
                                    'Jan', 'Feb', 'Mar', '2024', '2025']
                        found_indicators = [ind for ind in indicators if ind in text]
                        self.logger.info(f"Key indicators found: {found_indicators}")
                    else:
                        self.logger.warning("No text extracted")
                    
                    # Word extraction with positions
                    words = page.extract_words()
                    self.logger.info(f"Words extracted: {len(words)}")
                    
                    if words:
                        self.logger.info("Sample words with positions:")
                        for i, word in enumerate(words[:8]):
                            print(f"  '{word['text']}' at ({word['x0']:.1f}, {word['top']:.1f})")
                    
                    # Table extraction
                    tables = page.extract_tables()
                    self.logger.info(f"Tables detected: {len(tables)}")
                    
                    if tables:
                        for i, table in enumerate(tables[:1]):  # First table only
                            rows, cols = len(table), len(table[0]) if table else 0
                            self.logger.info(f"  Table {i+1}: {rows} rows x {cols} columns")
                            if table and table[0]:
                                self.logger.info(f"    Header sample: {table[0][:3]}")
        
        except Exception as e:
            self.logger.error(f"PDF analysis failed: {e}")
    
    def test_single_extraction(self, pdf_path, target_year, target_month_num):
        """Test extraction on a single PDF"""
        self.logger.info(f"🧪 TESTING SINGLE EXTRACTION")
        self.logger.info(f"File: {Path(pdf_path).name}")
        self.logger.info(f"Target: {target_year}-{target_month_num:02d}")
        self.logger.info("="*60)
        
        parser = PDFParser()
        result = parser.parse_monthly_brokerage_data(pdf_path, target_year, target_month_num)
        
        if "Error" in result:
            self.logger.error(f"Extraction failed: {result['Error']}")
        else:
            self.logger.success(f"Extraction successful: {len(result)} metrics found")
            self.logger.info("\nExtracted data:")
            for key, value in result.items():
                print(f"  {key}: {value}")
        
        return result

# ==============================================================================
# 7. MAIN APPLICATION ENTRY POINT
# ==============================================================================

def main():
    """Main application entry point"""
    logger = Logger()
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "debug" and len(sys.argv) > 2:
            # Debug mode: analyze PDF structure
            pdf_path = sys.argv[2]
            if not Path(pdf_path).exists():
                logger.error(f"PDF file not found: {pdf_path}")
                return
            
            debug_tools = DebugTools()
            debug_tools.analyze_pdf_structure(pdf_path)
            
        elif command == "test" and len(sys.argv) > 4:
            # Test mode: test extraction on single PDF
            pdf_path = sys.argv[2]
            target_year = sys.argv[3]
            target_month = int(sys.argv[4])
            
            if not Path(pdf_path).exists():
                logger.error(f"PDF file not found: {pdf_path}")
                return
            
            debug_tools = DebugTools()
            debug_tools.test_single_extraction(pdf_path, target_year, target_month)
            
        elif command == "help":
            # Help mode
            print("""
Interactive Brokers PDF Parser - Usage Guide

NORMAL OPERATION:
    python ibkr_parser.py
    
    Scans current directory for PDF pairs and processes them automatically.
    Expected file naming: YYYYMMMonthlyBrokerageData.pdf and YYYYMMMetricsPressRelease.pdf

DEBUGGING MODES:
    python ibkr_parser.py debug <pdf_path>
    
    Analyzes PDF structure to help troubleshoot parsing issues.
    Shows text extraction, word positions, and table detection results.
    
    python ibkr_parser.py test <pdf_path> <year> <month>
    
    Tests data extraction on a single PDF file.
    Example: python ibkr_parser.py test 202508MonthlyBrokerageData.pdf 2025 8
    
    python ibkr_parser.py help
    
    Shows this help message.

OUTPUT:
    Creates CSV files named: IBKR_DATA_OUTPUT_YYYYMM.csv
    Contains 20 standardized financial metrics per month.
            """)
        else:
            logger.error("Invalid command line arguments")
            logger.info("Use 'python ibkr_parser.py help' for usage information")
    else:
        # Normal operation: batch processing
        logger.info("Starting Interactive Brokers Data Processing")
        logger.info("="*80)
        
        file_manager = FileManager()
        total_pairs, successful = file_manager.find_and_process_all_reports()
        
        logger.info("="*80)
        logger.info("PROCESSING COMPLETE")
        logger.info(f"Summary: {successful}/{total_pairs} pairs processed successfully")
        
        if successful > 0:
            logger.success("Check your directory for IBKR_DATA_OUTPUT_*.csv files")
        else:
            logger.warning("No data was processed. Check PDF file naming and content.")

if __name__ == "__main__":
    main()