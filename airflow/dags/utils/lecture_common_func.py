import pandas as pd
import requests
import logging
from utils.kakaomap_api import fetch_geocding
# from airflow.models import Variable

logger = logging.getLogger("airflow.task")

def filter_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    LCTR_NM에 '&amp;' 포함 시 ' ' (공백)으로 변환
    LCTR_NM에 '스마트폰' OR '키오스크' OR '컴퓨터' 포함된 데이터만 대상 (포함 조건)
    LCTR_NM에 '자격증' OR '컴퓨터활용능력' OR 'ITQ' 포함된 데이터는 제외 (제외 조건)
    """
    if 'LCTR_NM' not in df.columns:
        return pd.DataFrame() # LCTR_NM이 없으면 빈 데이터프레임 반환

    df_temp = df.copy()
    df_temp['LCTR_NM'] = df_temp['LCTR_NM'].astype(str).str.replace('&amp;', ' ')

    lctr_nm_series = df_temp['LCTR_NM'].astype(str).str.upper()

    # 1. 포함 조건: 스마트폰, 키오스크, 컴퓨터 중 하나라도 포함
    include_keywords = ['스마트폰', '키오스크', '컴퓨터']
    include_condition = lctr_nm_series.apply(
        lambda x: any(k in x for k in include_keywords)
    )

    # 2. 제외 조건: 자격증, 컴퓨터활용능력, ITQ 중 하나라도 포함
    exclude_keywords = ['자격증', '컴퓨터활용능력', 'ITQ']
    exclude_condition = lctr_nm_series.apply(
        lambda x: any(k in x for k in exclude_keywords)
    )

    # 최종 필터링: 포함 조건은 만족, 제외 조건은 만족하지 않는 행만 선택
    filtered_df = df_temp[include_condition & ~exclude_condition].copy()
    
    return filtered_df


def map_and_explode_categories(df: pd.DataFrame) -> pd.DataFrame:
    """
    디지털 배움터 (종료) 강좌 테이블의 DL_NM 컬럼을 LECTURE 테이블의 ADDRESS, DL_NM으로 분리 파싱.
    """
    if df.empty:
        return df
        
    # 1. 카테고리 매핑 로직
    def get_categories(lctr_nm):
        categories = []
        lctr_nm = str(lctr_nm).upper().replace(" ", "")
        
        # 키오스크 매핑
        if '키오스크' in lctr_nm:
            categories.append('키오스크')
            
        # 컴퓨터 매핑
        if '컴퓨터' in lctr_nm:
            categories.append('컴퓨터')
            
        # 스마트폰 매핑
        if '스마트폰' in lctr_nm:
            # 기초, 조작, 초급 확인
            if any(k in lctr_nm for k in ['기초', '조작', '초급']):
                categories.append('기초 스마트폰')
            else:
                categories.append('스마트폰 활용')
                
        # 필터링은 통과했지만 카테고리 키워드 매핑에서 누락될 경우
        if not categories:
            categories.append(None)
            
        return categories

    # 'CATEGORIES'라는 임시 컬럼에 각 행이 해당하는 카테고리 리스트를 저장
    df['CATEGORIES'] = df['LCTR_NM'].apply(get_categories)
    
    # 2. 다중 카테고리일 경우 Row 복제
    df_exploded = df.explode('CATEGORIES')
    
    # 3. 새로운 LCTR_CATEGORY 컬럼에 CATEGORIES 값을 할당하고 임시 컬럼 삭제
    df_exploded['LCTR_CATEGORY'] = df_exploded['CATEGORIES']
    
    # 4. 카테고리가 None인 Row는 LCTR_CATEGORY가 None
    return df_exploded.drop(columns=['CATEGORIES'])


def parse_dl_nm_and_address(raw_str: str):
    """
    디지털 배움터 (종료) 강좌 테이블의 DL_NM 컬럼을 LECTURE 테이블의 ADDRESS, DL_NM으로 분리 파싱.
    """
    if not raw_str or pd.isna(raw_str):
        return None, None

    raw_str = str(raw_str).strip()

    # 첫 번째 닫는 괄호
    first_close_idx = raw_str.find(')')
    if first_close_idx == -1:
        return None, raw_str

    # 두 번째 닫는 괄호
    second_close_idx = raw_str.find(')', first_close_idx + 1)

    # -----------------------------
    # CASE 1: 두 번째 ')'가 있는 경우 (기존 로직)
    # -----------------------------
    if second_close_idx != -1:
        address = raw_str[:second_close_idx + 1].strip()

        # 두 번째 ')' 이후 문자열
        rest = raw_str[second_close_idx + 1:]
        dl_nm = rest.split('위치보기')[0].strip()

        return address, dl_nm if dl_nm else None

    # -----------------------------
    # CASE 2: 두 번째 ')'가 없는 경우
    # → 공백 2칸 기준 분리
    # -----------------------------
    if "  " in raw_str:
        addr_part, dl_nm_part = raw_str.split("  ", 1)
        address = addr_part.strip()
        dl_nm = dl_nm_part.split('위치보기')[0].strip()

        return address, dl_nm if dl_nm else None
    
    # fallback
    return None, raw_str


def fetch_road_address(keyword: str, api_key: str, api_url: str) -> str:
    """
    경기도 평생학습 테이블에서 사용, 주소 검색 API를 호출하여 도로명 주소를 반환.
    """

    # 주소 찾기 api
    if not keyword or pd.isna(keyword):
        return None

    params = {
        "confmKey": api_key,
        "currentPage": 1,
        "countPerPage": 10,
        "keyword": keyword,
        "resultType": "json"
    }

    try:
        res = requests.get(api_url, params=params, timeout=5)
        res.raise_for_status()
        data = res.json()

        common = data.get("results", {}).get("common", {})
        if common.get("errorCode") != "0":
            return None

        if int(common.get("totalCount", 0)) == 0:
            return None

        juso_list = data["results"].get("juso", [])
        if not juso_list:
            return None

        # 대원1동 행정복지센터 예외 처리
        if keyword == "대원1동 행정복지센터":
            if len(juso_list) > 1:
                return juso_list[1].get("roadAddr")
            return None
        
        return juso_list[0].get("roadAddr")

    except Exception as e:
        logger.warning(f"[주소 조회 실패] keyword={keyword}, error={e}")
        return None
    

def resolve_lc_and_coord(address: str, api_key: str):
    """
    카카오 API 호출, ADDRESS 값으로 시도/시군구/읍면동 및 좌표(x,y) 반환.
    """
    data = fetch_geocding(address, api_key, max_retry=3)
    
    if not data or not data.get("documents"):
        return None, None, None, None, None
    
    doc = data["documents"][0]

    # address 우선, 없으면 road_address
    addr = doc.get("address") or doc.get("road_address")
    if not addr:
        return None, None, None, None, None

    lc1 = addr.get("region_1depth_name")
    lc2 = addr.get("region_2depth_name")
    lc3 = addr.get("region_3depth_name")
    x = addr.get("x") if addr.get("x") else None
    y = addr.get("y") if addr.get("y") else None

    return lc1, lc2, lc3, x, y