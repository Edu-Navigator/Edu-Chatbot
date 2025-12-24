from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def get_driver():
    """
    크롤링을 위한 webdriver 연결
    
    Returns
    -------
    webdriver
        chrome 드라이버
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    driver = webdriver.Chrome(options=chrome_options)
    return driver