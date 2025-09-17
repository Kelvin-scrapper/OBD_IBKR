#!/usr/bin/env python3
"""
IBKR Data Processing Orchestrator
================================

Complete workflow orchestrator for Interactive Brokers data processing:
1. Download PDFs from IBKR website (main.py)
2. Extract and process data (map2.py)
3. Generate standardized CSV output
4. Handle errors and provide comprehensive logging

Author: Interactive Brokers Data Processing Team
Version: 1.0
Date: 2025

Usage:
    python orchestrator.py                    # Full workflow
    python orchestrator.py --download-only   # Download PDFs only
    python orchestrator.py --process-only    # Process existing PDFs only
    python orchestrator.py --help           # Show help
"""

import os
import sys
import argparse
import subprocess
import time
from pathlib import Path
import logging
from datetime import datetime

class IBKROrchestrator:
    """Main orchestrator class for IBKR data processing workflow"""

    def __init__(self, working_dir=None, verbose=False):
        self.working_dir = Path(working_dir) if working_dir else Path.cwd()
        self.downloads_dir = self.working_dir / "downloads"
        self.output_dir = self.working_dir
        self.verbose = verbose

        # Setup logging
        self.setup_logging()

        # Ensure directories exist
        self.downloads_dir.mkdir(exist_ok=True)

    def setup_logging(self):
        """Setup logging configuration"""
        log_level = logging.DEBUG if self.verbose else logging.INFO

        # Create logs directory
        logs_dir = self.working_dir / "logs"
        logs_dir.mkdir(exist_ok=True)

        # Setup logging format
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # File handler
        log_file = logs_dir / f"ibkr_orchestrator_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))

        # Setup logger
        self.logger = logging.getLogger('IBKROrchestrator')
        self.logger.setLevel(log_level)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        self.logger.info(f"Logging initialized. Log file: {log_file}")

    def check_dependencies(self):
        """Check if required dependencies are installed"""
        self.logger.info("Checking dependencies...")

        required_modules = [
            'pdfplumber',
            'undetected_chromedriver',
            'selenium',
            'pathlib'
        ]

        missing_modules = []
        for module in required_modules:
            try:
                __import__(module)
                self.logger.debug(f"✓ {module} - OK")
            except ImportError:
                missing_modules.append(module)
                self.logger.error(f"✗ {module} - MISSING")

        if missing_modules:
            self.logger.error("Missing required modules. Install with:")
            self.logger.error(f"pip install {' '.join(missing_modules)}")
            return False

        self.logger.info("All dependencies satisfied")
        return True

    def check_required_files(self):
        """Check if required script files exist"""
        self.logger.info("Checking required files...")

        required_files = [
            self.working_dir / "main.py",
            self.working_dir / "map2.py"
        ]

        missing_files = []
        for file_path in required_files:
            if file_path.exists():
                self.logger.debug(f"✓ {file_path.name} - OK")
            else:
                missing_files.append(str(file_path))
                self.logger.error(f"✗ {file_path.name} - MISSING")

        if missing_files:
            self.logger.error("Missing required files:")
            for file in missing_files:
                self.logger.error(f"  - {file}")
            return False

        self.logger.info("All required files present")
        return True

    def download_pdfs(self):
        """Execute main.py to download PDFs from IBKR website"""
        self.logger.info("="*60)
        self.logger.info("STEP 1: Downloading PDFs from IBKR website")
        self.logger.info("="*60)

        try:
            # Check if main.py exists
            main_script = self.working_dir / "main.py"
            if not main_script.exists():
                self.logger.error("main.py not found")
                return False

            # Execute main.py
            self.logger.info("Executing main.py...")
            result = subprocess.run(
                [sys.executable, str(main_script)],
                cwd=str(self.working_dir),
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )

            # Log output
            if result.stdout:
                self.logger.info("main.py output:")
                for line in result.stdout.split('\n'):
                    if line.strip():
                        self.logger.info(f"  {line}")

            if result.stderr:
                self.logger.warning("main.py errors:")
                for line in result.stderr.split('\n'):
                    if line.strip():
                        self.logger.warning(f"  {line}")

            if result.returncode == 0:
                self.logger.info("PDF download completed successfully")
                return True
            else:
                self.logger.error(f"PDF download failed with return code: {result.returncode}")
                return False

        except subprocess.TimeoutExpired:
            self.logger.error("PDF download timed out after 5 minutes")
            return False
        except Exception as e:
            self.logger.error(f"Error during PDF download: {e}")
            return False

    def check_downloaded_pdfs(self):
        """Check if PDFs were successfully downloaded"""
        self.logger.info("Checking for downloaded PDFs...")

        pdf_files = list(self.downloads_dir.glob("*.pdf"))

        if not pdf_files:
            self.logger.warning("No PDF files found in downloads directory")
            return False

        self.logger.info(f"Found {len(pdf_files)} PDF files:")
        for pdf_file in pdf_files:
            file_size = pdf_file.stat().st_size
            self.logger.info(f"  ✓ {pdf_file.name} ({file_size:,} bytes)")

        # Check for expected PDF types
        expected_types = ["MonthlyBrokerageData", "MetricsPressRelease"]
        found_types = []

        for pdf_file in pdf_files:
            for pdf_type in expected_types:
                if pdf_type.lower() in pdf_file.name.lower():
                    found_types.append(pdf_type)
                    break

        if len(found_types) >= 2:
            self.logger.info("Found both required PDF types")
            return True
        else:
            self.logger.warning(f"Missing PDF types. Found: {found_types}, Expected: {expected_types}")
            return True  # Continue processing with available PDFs

    def process_pdfs(self):
        """Execute map2.py to process downloaded PDFs"""
        self.logger.info("="*60)
        self.logger.info("STEP 2: Processing PDFs and extracting data")
        self.logger.info("="*60)

        try:
            # Check if map2.py exists
            map_script = self.working_dir / "map2.py"
            if not map_script.exists():
                self.logger.error("map2.py not found")
                return False

            # Execute map2.py
            self.logger.info("Executing map2.py...")
            result = subprocess.run(
                [sys.executable, str(map_script)],
                cwd=str(self.working_dir),
                capture_output=True,
                text=True,
                timeout=120  # 2 minute timeout
            )

            # Log output
            if result.stdout:
                self.logger.info("map2.py output:")
                for line in result.stdout.split('\n'):
                    if line.strip():
                        self.logger.info(f"  {line}")

            if result.stderr:
                self.logger.warning("map2.py errors:")
                for line in result.stderr.split('\n'):
                    if line.strip():
                        self.logger.warning(f"  {line}")

            if result.returncode == 0:
                self.logger.info("PDF processing completed successfully")
                return True
            else:
                self.logger.error(f"PDF processing failed with return code: {result.returncode}")
                return False

        except subprocess.TimeoutExpired:
            self.logger.error("PDF processing timed out after 2 minutes")
            return False
        except Exception as e:
            self.logger.error(f"Error during PDF processing: {e}")
            return False

    def check_output_files(self):
        """Check if output CSV files were generated"""
        self.logger.info("Checking for output files...")

        # Look for IBKR_DATA_OUTPUT_*.csv files
        output_files = list(self.working_dir.glob("IBKR_DATA_OUTPUT_*.csv"))

        if not output_files:
            self.logger.warning("No output CSV files found")
            return False

        self.logger.info(f"Found {len(output_files)} output files:")
        for output_file in output_files:
            file_size = output_file.stat().st_size
            self.logger.info(f"  ✓ {output_file.name} ({file_size:,} bytes)")

        return True

    def cleanup_old_files(self, keep_days=7):
        """Clean up old log files and temporary files"""
        self.logger.info("Cleaning up old files...")

        try:
            logs_dir = self.working_dir / "logs"
            if logs_dir.exists():
                cutoff_time = time.time() - (keep_days * 24 * 60 * 60)

                for log_file in logs_dir.glob("*.log"):
                    if log_file.stat().st_mtime < cutoff_time:
                        log_file.unlink()
                        self.logger.debug(f"Deleted old log file: {log_file.name}")

            self.logger.info("Cleanup completed")
        except Exception as e:
            self.logger.warning(f"Error during cleanup: {e}")

    def run_full_workflow(self):
        """Execute the complete IBKR data processing workflow"""
        self.logger.info("="*80)
        self.logger.info("IBKR DATA PROCESSING WORKFLOW STARTED")
        self.logger.info("="*80)

        start_time = time.time()

        # Step 0: Prerequisites
        if not self.check_dependencies():
            self.logger.error("Dependency check failed")
            return False

        if not self.check_required_files():
            self.logger.error("File check failed")
            return False

        # Step 1: Download PDFs
        if not self.download_pdfs():
            self.logger.error("PDF download step failed")
            return False

        if not self.check_downloaded_pdfs():
            self.logger.error("PDF download verification failed")
            return False

        # Step 2: Process PDFs
        if not self.process_pdfs():
            self.logger.error("PDF processing step failed")
            return False

        if not self.check_output_files():
            self.logger.warning("Output verification failed - but continuing")

        # Step 3: Cleanup
        self.cleanup_old_files()

        # Summary
        end_time = time.time()
        duration = end_time - start_time

        self.logger.info("="*80)
        self.logger.info("WORKFLOW COMPLETED SUCCESSFULLY")
        self.logger.info(f"Total execution time: {duration:.2f} seconds")
        self.logger.info("="*80)

        return True

    def run_download_only(self):
        """Execute only the PDF download step"""
        self.logger.info("DOWNLOAD-ONLY MODE")

        if not self.check_dependencies():
            return False

        if not self.check_required_files():
            return False

        return self.download_pdfs() and self.check_downloaded_pdfs()

    def run_process_only(self):
        """Execute only the PDF processing step"""
        self.logger.info("PROCESS-ONLY MODE")

        if not self.check_dependencies():
            return False

        if not self.check_required_files():
            return False

        if not self.check_downloaded_pdfs():
            self.logger.error("No PDFs found to process")
            return False

        return self.process_pdfs() and self.check_output_files()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='IBKR Data Processing Orchestrator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python orchestrator.py                    # Run complete workflow
  python orchestrator.py --download-only   # Download PDFs only
  python orchestrator.py --process-only    # Process existing PDFs only
  python orchestrator.py --verbose         # Run with detailed logging
  python orchestrator.py --directory /path # Run in specific directory

The orchestrator manages the complete IBKR data processing workflow:
1. Downloads latest PDFs from IBKR website
2. Extracts financial metrics using universal parsing
3. Generates standardized CSV output files
4. Provides comprehensive logging and error handling
        """)

    parser.add_argument('--download-only', action='store_true',
                       help='Execute only PDF download step')
    parser.add_argument('--process-only', action='store_true',
                       help='Execute only PDF processing step')
    parser.add_argument('--directory', '-d',
                       help='Working directory (default: current directory)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')

    args = parser.parse_args()

    # Validate arguments
    if args.download_only and args.process_only:
        print("ERROR: Cannot specify both --download-only and --process-only")
        return 1

    # Initialize orchestrator
    orchestrator = IBKROrchestrator(
        working_dir=args.directory,
        verbose=args.verbose
    )

    # Execute requested workflow
    try:
        if args.download_only:
            success = orchestrator.run_download_only()
        elif args.process_only:
            success = orchestrator.run_process_only()
        else:
            success = orchestrator.run_full_workflow()

        return 0 if success else 1

    except KeyboardInterrupt:
        orchestrator.logger.info("Workflow interrupted by user")
        return 1
    except Exception as e:
        orchestrator.logger.error(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())