import time
import requests


def fetch_geocding(addr, api_key, max_retry=3):
    '''
    Docstring for fetch_geocding
    
    주소 -> 좌표 변환 API 호출
    
    :param addr: 주소 
    :param api_key: 카카오 REST API 키
    :param max_retry: (default = 3) API 재시도 호출 횟수
    '''
    # 호출시 필요한 변수 및 쿼리
    url     = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {"Authorization": f"KakaoAK {api_key}"}
    params  = {"query": addr}
    
    for attempt in range(max_retry):
        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=2
            )

            # HTTP 정상응답 체크
            if response.status_code != 200:
                print(f"[{addr}] HTTP {response.status_code} (retry {attempt+1})")
                time.sleep(0.2)
                continue

            data = response.json()

            if not data.get("documents"):
                # documents = [] 인 경우 (일치 주소 없음)
                return None

            return data

        except requests.exceptions.RequestException as e:
            print(f"[{addr}] Exception: {e} (retry {attempt+1})")
            time.sleep(0.2)
            continue

    return None
