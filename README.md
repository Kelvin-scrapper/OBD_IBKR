# IBKR Data Processing System

## Overview

A comprehensive automated system for downloading, processing, and extracting financial metrics from Interactive Brokers (IBKR) monthly reports and press releases. The system converts PDF data into standardized CSV format for analysis and reporting.

## Features

### 🚀 **Complete Workflow Automation**
- **Automated PDF Download**: Downloads latest PDFs from IBKR website using web scraping
- **Universal PDF Processing**: Extracts data from any month/year with fallback date detection
- **Standardized Output**: Generates consistent CSV format for 20+ financial metrics
- **Error Handling**: Comprehensive logging and recovery mechanisms

### 🔧 **Universal & Robust Design**
- **Order-Independent Processing**: Works regardless of PDF layout changes
- **Dynamic Column Detection**: Automatically finds correct data columns
- **Fallback Date Detection**: Extracts dates from PDF content when filenames lack dates
- **Multiple Parsing Strategies**: Text-based, coordinate-based, and table-based extraction

### 📊 **Data Processing Capabilities**
- **20+ Financial Metrics**: Account levels, trading volumes, DARTs, commissions, etc.
- **Calculated Fields**: Automatic calculation of derived metrics (e.g., Cash % of Assets)
- **Footnote Handling**: Processes metrics with footnotes like `Client Credits(3)`
- **Future-Proof**: Handles data for years 2015-2030

## Quick Start

### Prerequisites
- Python 3.8 or higher
- Chrome browser (version 110+ recommended)
- Windows 10/11 (tested environment)

### Installation
```bash
# Clone or download the project files
cd OBD_IBKR

# Install dependencies
pip install -r requirements.txt

# Verify installation
python orchestrator.py --help
```

### Basic Usage
```bash
# Complete workflow (download + process)
python orchestrator.py

# Download PDFs only
python orchestrator.py --download-only

# Process existing PDFs only
python orchestrator.py --process-only

# Run with detailed logging
python orchestrator.py --verbose
```

## File Structure

```
OBD_IBKR/
├── orchestrator.py          # Main workflow orchestrator
├── main.py                  # PDF downloader (web scraping)
├── map2.py                  # Universal PDF processor
├── requirements.txt         # Python dependencies
├── README.md               # This documentation
├── downloads/              # Downloaded PDF files
├── logs/                   # Execution logs
└── IBKR_DATA_OUTPUT_*.csv  # Generated output files
```

## Components

### 1. **orchestrator.py** - Workflow Manager
- Coordinates the complete data processing pipeline
- Handles dependency checking and error recovery
- Provides comprehensive logging and monitoring
- Supports different execution modes (full, download-only, process-only)

**Usage:**
```bash
python orchestrator.py [OPTIONS]

Options:
  --download-only    Execute only PDF download step
  --process-only     Execute only PDF processing step
  --directory DIR    Working directory (default: current)
  --verbose         Enable detailed logging
  --help            Show help message
```

### 2. **main.py** - PDF Downloader
- Automated web scraping from IBKR monthly metrics page
- Handles anti-bot detection and popups
- Downloads both Monthly Brokerage Data and Press Release PDFs
- Configurable headless/visible browser mode

**Direct Usage:**
```bash
python main.py
```

### 3. **map2.py** - Universal PDF Processor
- Extracts financial metrics from IBKR PDFs
- Universal design works with any month/year data
- Multiple parsing strategies for maximum reliability
- Fallback date detection from PDF content

**Direct Usage:**
```bash
python map2.py                                    # Process all PDFs in downloads/
python map2.py debug <pdf_path>                  # Debug PDF structure
python map2.py test <pdf_path> <year> <month>    # Test single PDF extraction
python map2.py help                              # Show detailed help
```

## Output Format

The system generates CSV files with the following structure:

### **File Naming**
- `IBKR_DATA_OUTPUT_YYYYMM.csv` (e.g., `IBKR_DATA_OUTPUT_202508.csv`)

### **CSV Structure**
- **Row 1**: Column headers (standardized field names)
- **Row 2**: Descriptive headers (human-readable descriptions)
- **Row 3**: Data values for the target month

### **Extracted Metrics** (20 fields)
| Category | Metrics | Source |
|----------|---------|---------|
| **Accounts** | Total Accounts, Net New Accounts | Monthly Brokerage |
| **DARTs** | Total Client DARTs, Cleared Client DARTs, Avg DART per Account | Monthly Brokerage |
| **Trading Volumes** | Options Contracts, Futures Contracts, Stock Shares | Monthly Brokerage |
| **Account Levels** | Client Equity, FDIC Credits, Credits at Broker, Total Credits, Margin Loans, Cash % | Monthly Brokerage |
| **Commissions** | Stock Commission, Options Commission, Futures Commission | Press Release |
| **Order Sizes** | Stock Order Size, Options Order Size, Futures Order Size | Press Release |

### **Sample Output**
```csv
Date,USA.OBD.INTERACTIVE.ACCOUNTS.TOTAL.M,USA.OBD.INTERACTIVE.ACCOUNTS.NETNEW.M,...
,"From Monthly Metrics: Accounts: Total Accounts: Thousands","From Monthly Metrics: Accounts: Net New Accounts: Thousands",...
2025-08,4054.2,96.1,3488,,187,135763,17186,36238967,713.2,6.0,140.4,146.4,71.8,20.5272,2.00,3.81,4.28,969,6.6,3.1
```

## Advanced Features

### **Universal Processing**
- **Date Detection**: Automatically detects target year/month from filenames or PDF content
- **Layout Independence**: Works regardless of PDF column/row order changes
- **Dynamic Header Mapping**: Finds correct data columns even if header structure changes
- **Footnote Handling**: Processes metrics with footnotes (e.g., `Client Credits(3)`)

### **Fallback Mechanisms**
- **Content-Based Date Extraction**: When filenames lack dates, extracts from PDF content
- **Multiple Parsing Strategies**: Text → Coordinate → Table extraction fallbacks
- **Error Recovery**: Continues processing even when some data is missing

### **Calculated Fields**
- **Cash as % of Assets**: `(Client Credits Total / Client Equity) × 100`
- **Automatic Calculation**: Updates dynamically based on extracted values

## Configuration

### **Download Settings** (main.py)
```python
HEADLESS_MODE = True          # Browser visibility
IBKR_METRICS_URL = "..."     # Target URL
```

### **Processing Settings** (map2.py)
```python
# Year validation range
YEAR_RANGE = (2015, 2030)

# Output column order
FINAL_CSV_COLUMNS = [...]

# Metric mapping configuration
MAPPING_CONFIG = {...}
```

## Troubleshooting

### **Common Issues**

#### 1. **No PDFs Downloaded**
```bash
# Check Chrome installation
chrome --version

# Run with visible browser
# Edit main.py: HEADLESS_MODE = False

# Check downloads directory
ls downloads/
```

#### 2. **PDF Processing Errors**
```bash
# Debug PDF structure
python map2.py debug downloads/202508MonthlyBrokerageData.pdf

# Test specific extraction
python map2.py test downloads/202508MonthlyBrokerageData.pdf 2025 8

# Check PDF content
python -c "import pdfplumber; pdf = pdfplumber.open('downloads/file.pdf'); print(pdf.pages[0].extract_text())"
```

#### 3. **Missing Dependencies**
```bash
# Reinstall requirements
pip install --upgrade -r requirements.txt

# Check Chrome driver
python -c "import undetected_chromedriver as uc; uc.Chrome().quit()"
```

#### 4. **Permission Errors**
```bash
# Run as administrator (Windows)
# Or change working directory permissions

# Use different output directory
python orchestrator.py --directory /different/path
```

### **Debug Mode**
Enable verbose logging for detailed troubleshooting:
```bash
python orchestrator.py --verbose
```

Check log files in `logs/` directory for detailed execution traces.

## Development

### **Project Structure**
```
src/
├── orchestrator.py    # Main workflow coordinator
├── main.py           # Web scraping module
├── map2.py           # PDF processing engine
├── requirements.txt  # Dependencies
└── README.md        # Documentation
```

### **Adding New Metrics**
1. Update `MAPPING_CONFIG` in map2.py
2. Add metric patterns to parsing logic
3. Update `FINAL_CSV_COLUMNS` order
4. Test with sample PDFs

### **Extending Functionality**
- Add new PDF sources by extending main.py
- Support additional date ranges in validation logic
- Add new parsing strategies in map2.py
- Implement additional output formats

## Version History

### **Version 1.0** (Current)
- Complete workflow automation
- Universal PDF processing with fallback detection
- 20+ financial metrics extraction
- Comprehensive logging and error handling
- Multi-mode execution (full/download/process)

## Support

### **Requirements**
- Python 3.8+
- Chrome Browser 110+
- Windows 10/11 (tested)
- ~50MB disk space

### **Performance**
- Download time: 30-60 seconds (depends on network)
- Processing time: 5-15 seconds per PDF pair
- Memory usage: <100MB typical

### **Limitations**
- Requires Chrome browser for web scraping
- IBKR website changes may affect downloading
- PDF format changes may require pattern updates

## License

This project is designed for legitimate financial data processing and analysis purposes. Use responsibly and in accordance with Interactive Brokers' terms of service.

---

**Author**: Interactive Brokers Data Processing Team
**Version**: 1.0
**Last Updated**: September 2025

For issues or feature requests, please check the troubleshooting section or review the comprehensive logging output.