import time
import re, requests
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from io import BytesIO
from PIL import Image

from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from utils import webdriver

import logging
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

@task
def scape_images(location_url, wait_time, **context):
    """
    location_url의 이미지 정보 크롤링하고 리스트로 반환한다

    location_url의 페이지별로
    교육 장소 이미지 링크, 교육 장소 명칭을 수집한다.
    
    Parameters
    ----------
    location_url : str
        이미지 크롤링할 교육 장소 url
    wait_time : int
        대기시간 (동적요소 올라올때까지 대기, 페이지 이동 후 대기 등)

    Returns
    -------
    list
        이미지 url과 장소명을 포함
        downstream task에서 입력으로 사용된다.

    Notes
    -----
    - 페이지네이션 처리 로직
        - URL의 페이지네이션은 마지막 페이지여도 "다음 페이지"버튼이 활성화 되어있다.
        - 현 페이지 번호와 다음 페이지 번호를 비교하여, 마지막 페이지를 탐지한다.
    """
    driver = webdriver.get_driver()
    
    # location_url = "https://www.xn--2z1bw8k1pjz5ccumkb.kr/edc/crse/place.do"
    all_images = []
    
    try :
        driver.get(location_url)
        logging.info("Starting image scraping...")
        
        while True:
            try :
                wait = WebDriverWait(driver, 10)
                wait.until(
                    EC.visibility_of_element_located(
                        (By.XPATH, '//*[@id="container"]/div/div[3]/a[1]')
                    )
                )
                time.sleep(wait_time*1.5) # 혹시 다른 이미지 요소 안올라올거 대비용
            except Exception as e:
                error_msg = f'Timeout waiting for page to load: {str(e)}'
                logging.info(error_msg)
                driver.quit()
                raise TimeoutError(error_msg)
            
            cur_page = driver.find_element(By.CSS_SELECTOR, '#page_no').get_attribute('value')
            
            # 현재 페이지의 이미지 정보 수집
            parent_item = driver.find_element(By.XPATH, '//*[@id="container"]/div/div[3]')
            items = parent_item.find_elements(By.CLASS_NAME, 'itemBox')
            
            page_images = []
            for item in items:
                try:
                    img_src = item.find_element(By.CSS_SELECTOR, ".imgArea img").get_attribute("src")
                    title   = item.find_element(By.CSS_SELECTOR, "p.title.ellip").text
                    page_images.append({
                        'url': img_src,
                        'title': title,
                    })
                except Exception as e:
                    logging.info(f"Error extracting item on page {cur_page}: {e}")
                    continue
                
            all_images.extend(page_images)
            logging.info(f"Page {cur_page}: Collected {len(page_images)} images (Total: {len(all_images)})")
            
            # 페이지 네비게이션 체크
            try:
                next_btn = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '#paging > div > a.btnNext'))
                )
                match = re.search(r"pno=(\d+)", next_btn.get_attribute('onclick'))
                next_page = match.group(1) if match else None
                
                if next_page == cur_page:
                    logging.info("Reached last page")
                    break
                
                next_btn.click()
                logging.info(f"Waiting {wait_time} seconds before next page...")
                time.sleep(wait_time)
                
            except Exception as e:
                error_msg = f"Navigation error on page {cur_page}: {str(e)}"
                driver.quit()
                raise RuntimeError(error_msg)
        
        logging.info(f"Scraping complete: {len(all_images)} images")
        
        return all_images
    
    finally:
        driver.quit()

@task
def upload_to_s3(image_list, bucket, base_key, batch_size=20, delay=5,conn_name='s3_conn', **context):
    """
    이미지를 s3에 업로드.

    scape_images 태스크로 수집된 이미지 정보를 input으로 사용하여
    s3에 resize 후 업로드를 진행한다.

    Parameters
    ----------
    image_list : list
        s3에 업로드할 image 정보를 포함하는 list
    bucket : str
        데이터 적재용 s3 버킷명
    batch_size : int
        default = 20
        이미지 다운로드 진행상황 출력 및 대기를 위해 설정
    delay : int
        default = 5
        batch_size의 이미지 다운 후 대기할 시간(second) 설정
    conn_name : str
        Airflow Connection에 등록된 s3 연결명


    Notes
    -----
    - 입력 데이터
        - upstream task에서 전달된 값을 사용한다.
    - 이미지 s3 저장
        - 고정된 사이즈의 이미지로 resize
        - 장소명을 image 파일명으로 사용
    - 실행 특성
        - s3 업로드 실패시 log만 남기고 다음 적재 대상으로 넘어간다.
        - 이미지는 부가요소이기 때문에, Error 발생시키지 않음
    """
    uploaded_count = 0
    failed_count = 0
    failed_images = []
    logging.info(f"Starting upload: {len(image_list)} images in batches of {batch_size}")
    
    s3hook = S3Hook(aws_conn_id=conn_name)
    s3client = s3hook.get_conn() # boto3 client

    for idx, img_info in enumerate(image_list, 1):
        try:
            img_url = img_info['url']
            title   = img_info['title']
            save_key = f"{base_key}/{title}.jpg"
            
            img_data = requests.get(img_url, timeout=15).content

            # 크기변형 후 적재 (카카오톡 제공을 위해..)
            img = Image.open(BytesIO(img_data))
            resized = img.resize((800, 400))
            buffer = BytesIO()
            resized.save(buffer, format=img.format)
            resized_bytes = buffer.getvalue()

            s3client.put_object( 
                Bucket=bucket,
                Key=save_key,
                Body=resized_bytes, 
                ContentType="image/jpeg"
            ) # 동일 key에 대해 자동으로 덮어쓰기

            uploaded_count += 1
            
            # 배치마다 진행상황 출력 및 대기
            if idx % batch_size == 0:
                logging.info(f"Progress: {idx}/{len(image_list)} uploaded")
                if idx < len(image_list):
                    logging.info(f"Waiting {delay} seconds before next batch...")
                    time.sleep(delay)
        except Exception as e:
            logging.info(f"Failed to upload image {idx} ({img_info.get('title', 'unknown')}): {e}")
            failed_count += 1
            failed_images.append({
                    'index': idx,
                    'title': img_info.get('title'),
                    'error': str(e)
                })
            continue
    
    logging.info(f"Upload complete: {uploaded_count} succeeded, {failed_count} failed")
