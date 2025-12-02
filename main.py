import os
import time
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Configuration
HEADLESS_MODE = True  # Set to False to see browser window
IBKR_METRICS_URL = "https://investors.interactivebrokers.com/en/general/about/monthly-metrics.php"  # Correct IBKR metrics page URL

def get_chrome_version():
    """Detect Chrome version"""
    try:
        driver = uc.Chrome(version_main=None)  # Auto-detect version
        version = driver.capabilities['browserVersion']
        driver.quit()
        return version
    except Exception as e:
        print(f"Could not detect Chrome version: {e}")
        return None

def find_and_download_pdfs(url):
    """Navigate to IBKR website and find PDFs in Most Recent Information section"""
    
    # Create downloads directory if it doesn't exist
    downloads_dir = "downloads"
    if not os.path.exists(downloads_dir):
        os.makedirs(downloads_dir)
    
    # Detect Chrome version
    print("Detecting Chrome version...")
    chrome_version = get_chrome_version()
    if chrome_version:
        print(f"Chrome version detected: {chrome_version}")
    else:
        print("Using auto-detection for Chrome version")
    
    # Setup undetected Chrome driver
    options = uc.ChromeOptions()
    if HEADLESS_MODE:
        options.add_argument("--headless=new")  # Use new headless mode
        print("Running in headless mode")
    else:
        print("Running with visible browser window")
    
    # Anti-detection and popup handling
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins-discovery")
    options.add_argument("--disable-web-security")
    options.add_argument("--allow-running-insecure-content")
    options.add_argument("--disable-popup-blocking")  # Allow popups
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-save-password-bubble")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Set download directory and popup preferences
    prefs = {
        "download.default_directory": os.path.abspath(downloads_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
        "profile.default_content_settings.popups": 0,  # Allow popups
        "profile.default_content_setting_values.notifications": 2,  # Block notifications
        "profile.managed_default_content_settings.popups": 1,  # Allow popups
        "profile.content_settings.exceptions.automatic_downloads.*.setting": 1  # Allow downloads
    }
    options.add_experimental_option("prefs", prefs)
    
    driver = None
    try:
        driver = uc.Chrome(options=options, version_main=None)  # Auto-detect Chrome version
        
        # Additional anti-detection measures
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.execute_script("delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;")
        driver.execute_script("delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;")
        driver.execute_script("delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;")
        
        print(f"Navigating to: {url}")
        driver.get(url)
        
        # Wait longer for page to load
        time.sleep(5)
        
        # Wait for page to load
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Handle various popups and overlays
        try:
            # Handle cookie banner
            cookie_buttons = [
                "//button[contains(text(), 'ACCEPT') or contains(text(), 'Accept') or contains(text(), 'accept')]",
                "//button[contains(@class, 'cookie') and (contains(text(), 'OK') or contains(text(), 'Accept'))]",
                "//a[contains(text(), 'Accept') or contains(text(), 'ACCEPT')]",
                "//*[@id='cookie-accept']",
                "//*[@class*='cookie-accept']"
            ]
            
            for selector in cookie_buttons:
                try:
                    cookie_btn = WebDriverWait(driver, 3).until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )
                    driver.execute_script("arguments[0].click();", cookie_btn)
                    print("Accepted cookies")
                    time.sleep(2)
                    break
                except:
                    continue
        except:
            print("No cookie banner found or already handled")
        
        # Handle any other popups/modals
        try:
            close_buttons = [
                "//button[contains(@class, 'close')]",
                "//button[contains(@aria-label, 'close')]",
                "//*[@class*='modal-close']",
                "//*[@class*='popup-close']"
            ]
            
            for selector in close_buttons:
                try:
                    close_btn = driver.find_element(By.XPATH, selector)
                    if close_btn.is_displayed():
                        driver.execute_script("arguments[0].click();", close_btn)
                        print("Closed popup/modal")
                        time.sleep(1)
                except:
                    continue
        except:
            pass
        
        # Scroll to make sure content is loaded
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(2)
        
        print("Page loaded. Searching for PDF download links...")
        
        # Find the "Most Recent Information" section
        most_recent_section = None
        try:
            # Try multiple variations of finding the section
            selectors = [
                "//h3[contains(text(), 'Most Recent Information')]",
                "//h3[contains(text(), 'Most Recent')]",
                "//h4[contains(text(), 'Latest Press Release')]",
                "//h4[contains(text(), 'Historical Brokerage Metrics')]",
                "//a[contains(@href, 'latestMetricPR')]",
                "//a[contains(@href, 'latestMetric')]"
            ]
            
            for selector in selectors:
                try:
                    element = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, selector))
                    )
                    print(f"Found element with selector: {selector}")
                    # Get the parent container that contains the PDFs
                    most_recent_section = element.find_element(By.XPATH, "./ancestor::div[contains(@class, 'container') or contains(@class, 'row') or contains(@class, 'col')]")
                    break
                except:
                    continue
            
            if not most_recent_section:
                # Last resort: look for any links with the specific PDF URLs
                print("Trying direct link search...")
                download_links = driver.find_elements(By.XPATH, "//a[contains(@href, 'getFileNew.php')]")
                if download_links:
                    print(f"Found {len(download_links)} direct download links")
                    most_recent_section = driver  # Use the whole page
            
        except TimeoutException:
            print("Could not find any recognizable elements")
        
        # Find all PDF download links in this section
        pdf_links = []
        try:
            # Look for links that contain 'getFileNew.php'
            download_links = most_recent_section.find_elements(By.XPATH, ".//a[contains(@href, 'getFileNew.php')]")
            
            for link in download_links:
                href = link.get_attribute('href')
                if href:
                    # Try to find the description from nearby h4 element
                    try:
                        # Look for h4 element in the same highlight-box
                        highlight_box = link.find_element(By.XPATH, "./ancestor::div[contains(@class, 'highlight-box')]")
                        h4_element = highlight_box.find_element(By.TAG_NAME, "h4")
                        description = h4_element.text.split('\n')[0].strip()  # Get first line, remove date
                    except:
                        # Fallback: determine description from URL
                        if 'latestMetricPR' in href:
                            description = "Latest Press Release"
                        elif 'latestMetric' in href:
                            description = "Historical Brokerage Metrics"
                        else:
                            description = "PDF Document"
                    
                    pdf_links.append({
                        'url': href,
                        'description': description,
                        'element': link
                    })
            
        except Exception as e:
            print(f"Error finding PDF links: {e}")
        
        if not pdf_links:
            print("No PDF download links found in the 'Most Recent Information' section")
            return []
        
        print(f"Found {len(pdf_links)} PDF links:")
        for link in pdf_links:
            print(f"  - {link['description']}: {link['url']}")
        
        # Download each PDF
        downloaded_files = []
        for i, pdf_link in enumerate(pdf_links):
            try:
                print(f"\nDownloading file {i+1}/{len(pdf_links)}: {pdf_link['description']}")
                
                # Click the download link or navigate to URL
                if pdf_link['element']:
                    driver.execute_script("arguments[0].click();", pdf_link['element'])
                else:
                    driver.get(pdf_link['url'])
                
                print(f"Download initiated for: {pdf_link['description']}")
                time.sleep(5)  # Wait for download to start
                
                downloaded_files.append(pdf_link['description'])
                
            except Exception as e:
                print(f"Error downloading {pdf_link['description']}: {e}")
        
        # Wait for all downloads to complete
        print("\nWaiting for downloads to complete...")
        time.sleep(15)
        
        # Check downloaded files
        pdf_files = [f for f in os.listdir(downloads_dir) if f.endswith('.pdf')]
        if pdf_files:
            print(f"\nPDF files found in downloads directory:")
            for pdf_file in pdf_files:
                file_path = os.path.join(downloads_dir, pdf_file)
                file_size = os.path.getsize(file_path)
                print(f"  ✓ {pdf_file} ({file_size} bytes)")
        else:
            print("\nNo PDF files found in downloads directory")
        
        return downloaded_files
        
    except Exception as e:
        print(f"Error occurred: {e}")
        return []
    
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass  # Ignore cleanup errors

def main():
    print("IBKR PDF Downloader")
    print("=" * 50)
    print("Navigating to Interactive Brokers monthly metrics page...")
    print(f"URL: {IBKR_METRICS_URL}")
    print()
    
    downloaded_files = find_and_download_pdfs(IBKR_METRICS_URL)
    
    if downloaded_files:
        print(f"\nSuccessfully processed {len(downloaded_files)} files:")
        for file in downloaded_files:
            print(f"  - {file}")
    else:
        print("\nNo files were downloaded.")
    
    print("\nDownload process completed!")

if __name__ == "__main__":
    main()