import time
import requests


def fetch_geocding(addr, api_key, max_retry=3):
    """
    카카오 주소 검색 API를 호출하여 주소에 해당하는 좌표 정보를 조회한다.

    입력된 주소 문자열을 기준으로 카카오 로컬 API를 호출하여
    좌표 변환 결과를 반환한다. 네트워크 오류 또는 비정상 응답 발생 시
    지정된 횟수만큼 재시도를 수행한다.

    Parameters
    ----------
    addr : str
        좌표 조회에 사용할 주소 문자열.
    api_key : str
        카카오 REST API 인증 키.
    max_retry : int, optional
        API 호출 재시도 횟수 (기본값: 3).

    Returns
    -------
    dict or None
        API 응답 데이터(JSON 파싱 결과).
        검색 결과가 없거나 재시도 이후에도 실패한 경우 None을 반환한다.

    Raises
    ------
    requests.exceptions.RequestException
        HTTP 요청 과정에서 네트워크 오류가 발생할 수 있다.
    ValueError
        API 응답을 JSON으로 파싱하는 과정에서 오류가 발생할 수 있다.
    """
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
