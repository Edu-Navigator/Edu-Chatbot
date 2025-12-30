from airflow.models import Variable
import pandas as pd
from datetime import date
from utils.lecture_common_func import *


def process_suji(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    수지구청 평생학습 크롤링 데이터를 LECTURE 테이블 스키마에 맞게 변환한다.

    원본 DataFrame을 기반으로 공통 필터링 및 카테고리 매핑 로직을 적용하여 
    최종 LECTURE 형식의 DataFrame을 반환한다.

    Parameters
    ----------
    df_raw : pandas.DataFrame
        수지구청 평생학습 크롤링 결과 원본 데이터프레임.

    Returns
    -------
    pandas.DataFrame
        LECTURE 테이블 컬럼 구조에 맞게 변환된 데이터프레임.
        입력 데이터가 비어 있는 경우 빈 DataFrame을 반환한다.

    Raises
    ------
    KeyError
        필수 컬럼인 `lctr_nm`이 입력 데이터프레임에 존재하지 않으면
        카테고리 매핑 및 필터링 과정에서 오류가 발생할 수 있다.
    """

    if df_raw.empty:
        return pd.DataFrame()
    
    
    # [1단계] Raw 데이터 파싱 (LECTURE 스키마의 임시 DataFrame 구성)
    temp_df = pd.DataFrame(index=df_raw.index)

    # 1. APLY_URL
    temp_df['APLY_URL'] = df_raw['APLY_URL']

    # 2. LCTR_WAY (고정값: 오프라인)
    temp_df['LCTR_WAY'] = '오프라인'

    # 3. LCTR_CATEGORY (초기 NULL 처리 - 공통 함수에서 채워짐)
    temp_df['LCTR_CATEGORY'] = None

    # 4. LCTR_NM
    temp_df['LCTR_NM'] = df_raw['LCTR_NM']

    # 5. APLY_WAY (고정값: 온라인)
    temp_df['APLY_WAY'] = '온라인'
    
    # 6. APLY_BGN_END 파싱
    apply_bgn_str = df_raw['APLY_BGN_END'].str.split('~', expand=True)[0]
    apply_end_str = df_raw['APLY_BGN_END'].str.split('~', expand=True)[1]
    
    # 6-1. APLY_BGN (날짜만)
    apply_bgn_date_only = apply_bgn_str.str.split('(', expand=True)[0].str.strip()
    temp_df['APLY_BGN'] = pd.to_datetime(apply_bgn_date_only, errors='coerce')
    # 6-2. APLY_END (날짜만)
    apply_end_date_only = apply_end_str.str.split('(', expand=True)[0].str.strip()
    temp_df['APLY_END'] = pd.to_datetime(apply_end_date_only, errors='coerce')

    # 7. LCTR_BGN_END 파싱
    lctr_bgn_str = df_raw['LCTR_BGN_END'].str.split('~', expand=True)[0]
    lctr_end_str = df_raw['LCTR_BGN_END'].str.split('~', expand=True)[1]

    # 7-1. LCTR_BGN (날짜만)
    temp_df['LCTR_BGN'] = pd.to_datetime(lctr_bgn_str.str.strip(), errors='coerce')
    # 7-2. LCTR_END (날짜만)
    temp_df['LCTR_END'] = pd.to_datetime(lctr_end_str.str.split('(', expand=True)[0].str.strip(), errors='coerce')
    
    # 8. LCTR_CONTENT (강의명 재사용)
    temp_df['LCTR_CONTENT'] = df_raw['LCTR_NM']
    
    # 9. PCSP (정원 24로 고정)
    temp_df['PCSP'] = 24
    
    # 10. IS_APLY_AVL
    current_date = pd.to_datetime(date.today()) 
    temp_df['IS_APLY_AVL'] = (temp_df['APLY_END'].notna()) & (temp_df['APLY_END'] >= current_date)
    
    # 11. 주소 및 장소 관련 데이터 (우선 NULL 처리)
    temp_df['DL_NM'] = df_raw['LCTR_LOCATE']
    temp_df['ADDRESS'] = '경기도 용인시 수지구 포은대로 435 수지구청,수지보건소'
    temp_df['LC_1'] = None
    temp_df['LC_2'] = None
    temp_df['LC_3'] = None
    temp_df['ADDRESS_X'] = None
    temp_df['ADDRESS_Y'] = None
    
    # 12. LCTR_IMAGE
    temp_df['LCTR_IMAGE'] = None
    
    # [2단계] 공통 필터링 및 카테고리/ROW 분리 로직
    # 1. 강의 필터링
    df_filtered = filter_data(temp_df)
    
    # 2. 카테고리 매핑 및 Row 분리 (3번 조건)
    df_final = map_and_explode_categories(df_filtered)

    # LECTURE 테이블의 최종 컬럼 순서 조정
    final_columns = [
        'APLY_URL', 'LCTR_WAY', 'LCTR_CATEGORY', 'LCTR_NM', 'APLY_WAY', 
        'APLY_BGN', 'APLY_END', 'LCTR_BGN', 'LCTR_END', 'LCTR_CONTENT', 
        'PCSP', 'IS_APLY_AVL', 'DL_NM', 'ADDRESS', 'LC_1', 'LC_2', 'LC_3', 
        'ADDRESS_X', 'ADDRESS_Y', 'LCTR_IMAGE'
    ]
    return df_final[final_columns]


def process_gg(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    경기도 평생학습 OpenAPI 수집 데이터를 LECTURE 테이블 스키마에 맞게 변환한다.

    원본 DataFrame을 기반으로 공통 필터링 및 카테고리 매핑 로직을 적용하여 
    최종 LECTURE 형식의 DataFrame을 반환한다.

    Parameters
    ----------
    df_raw : pandas.DataFrame
        경기도 평생학습 OpenAPI 응답 원본 데이터프레임.

    Returns
    -------
    pandas.DataFrame
        LECTURE 테이블 컬럼 구조에 맞게 변환된 데이터프레임.
        입력 데이터가 비어 있는 경우 빈 DataFrame을 반환한다.

    Raises
    ------
    KeyError
        필수 컬럼인 `LCTR_NM`이 입력 데이터프레임에 존재하지 않으면
        공통 필터링 또는 카테고리 매핑 과정에서 오류가 발생할 수 있다.
    """

    if df_raw.empty:
        return pd.DataFrame()
    
    # [1단계] Raw 데이터 파싱 (LECTURE 스키마의 임시 DataFrame 구성)
    temp_df = pd.DataFrame(index=df_raw.index)

    # 1. APLY_URL
    temp_df['APLY_URL'] = df_raw['APLY_URL']
    
    # 2. LCTR_WAY (강의 방식) 매핑
    def map_lctr_way(way):
        if way in ['온라인']:
            return '온라인'
        elif way in ['오프라인']:
            return '오프라인'
        elif way in ['온라인+오프라인', '혼합']:
            return '혼합'
        return None # 매핑되지 않는 경우
        
    temp_df['LCTR_WAY'] = df_raw['LCTR_WAY'].apply(map_lctr_way)
    
    # 3. LCTR_CATEGORY (초기 NULL 처리 - 공통 함수에서 채워짐)
    temp_df['LCTR_CATEGORY'] = None
    
    # 4. LCTR_NM
    temp_df['LCTR_NM'] = df_raw['LCTR_NM']
    
    # 5. APLY_WAY (신청 방식) 매핑
    def map_aply_way(way):
        if way in ['인터넷', '온라인', '온라인접수']:
            return '온라인'
        elif way in ['방문', '방문접수']:
            return '방문접수'
        return '온라인 또는 방문접수' # 나머지
        
    temp_df['APLY_WAY'] = df_raw['APLY_WAY'].apply(map_aply_way)
    
    # 6, 7, 8, 9. 날짜 변환 (Raw 데이터는 '2025-03-04' 형태의 문자열)
    # pd.to_datetime을 사용하여 날짜 타입으로 일괄 변환
    temp_df['APLY_BGN'] = pd.to_datetime(df_raw['APLY_BGN'], errors='coerce')
    temp_df['APLY_END'] = pd.to_datetime(df_raw['APLY_END'], errors='coerce')
    temp_df['LCTR_BGN'] = pd.to_datetime(df_raw['LCTR_BGN'], errors='coerce')
    temp_df['LCTR_END'] = pd.to_datetime(df_raw['LCTR_END'], errors='coerce')
    
    # 10. LCTR_CONTENT 파싱 및 변환
    # '&amp;gt' -> ', ' 변환
    temp_df['LCTR_CONTENT'] = df_raw['LCTR_CONTENT'].astype(str).str.replace('&amp;gt', ', ', regex=False)

    # 11. PCSP
    temp_df['PCSP'] = pd.to_numeric(df_raw['LCTR_PCSP'], errors='coerce').fillna(0).astype(int)
    
    # 12. IS_APLY_AVL
    current_date = pd.to_datetime(date.today()) 
    # APLY_END가 오늘보다 작으면 (과거면) False, 아니면 True
    temp_df['IS_APLY_AVL'] = (temp_df['APLY_END'].notna()) & (temp_df['APLY_END'] >= current_date)
    
    # 13. DL_NM (교육 장소/운영처) 로직
    def map_dl_nm(operate, locate):
        operate = str(operate).strip()
        locate = str(locate).strip()
        # 1) LCTR_OPERATE == LCTR_LOCATE
        if operate == locate:
            return locate
        # 2) LCTR_OPERATE == ‘경기도 수원시평생학습관’
        if operate == '경기도 수원시평생학습관':
            return locate
        # 3) LCTR_OPERATE 앞의 3글자 == LCTR_LOCATE 앞의 3글자
        if operate[:3] == locate[:3] and len(operate) >= 3 and len(locate) >= 3:
            return locate
        # 4) 나머지
        return f"{operate} - {locate}"
        
    temp_df['DL_NM'] = df_raw.apply(
        lambda row: map_dl_nm(row['LCTR_OPERATE'], row['LCTR_LOCATE']), 
        axis=1
    )
    
    # 14. [2단계]에서 주소 검색 및 좌표 변환을 위해 LCTR_OPERATE, LCTR_LOCATE 보존
    temp_df['LCTR_OPERATE'] = df_raw['LCTR_OPERATE'] 
    temp_df['LCTR_LOCATE'] = df_raw['LCTR_LOCATE']

    # 15. 주소 및 장소 관련 데이터 (우선 NULL 처리)
    temp_df['ADDRESS'] = None
    temp_df['LC_1'] = None
    temp_df['LC_2'] = None
    temp_df['LC_3'] = None
    temp_df['ADDRESS_X'] = None
    temp_df['ADDRESS_Y'] = None
    temp_df['LCTR_IMAGE'] = None
    
    # [2단계] 공통 필터링 및 카테고리/ROW 분리 로직
    # 1. 강의 필터링 (필수)
    df_filtered = filter_data(temp_df)

    def resolve_gg_address(row) -> str | None:
        JUSO_API_KEY = Variable.get("juso_api_key")
        JUSO_API_URL = "https://business.juso.go.kr/addrlink/addrLinkApi.do"

        operate = str(row['LCTR_OPERATE']).strip()
        locate = str(row['LCTR_LOCATE']).strip()

        # 1차 키워드 결정
        if operate == '디지털배움터':
            primary_keyword = locate
        else:
            primary_keyword = operate

        # 1차 검색
        address = fetch_road_address(primary_keyword, JUSO_API_KEY, JUSO_API_URL)
        if address:
            return address

        # 2차 검색: locate 사용 (의미 없는 값 제외)
        if locate in ('평생학습관', '온라인'):
            return None

        return fetch_road_address(locate, JUSO_API_KEY, JUSO_API_URL)
    
    # df_filtered는 LCTR_OPERATE와 LCTR_LOCATE를 포함하므로 apply 가능
    df_filtered['ADDRESS'] = df_filtered.apply(resolve_gg_address, axis=1)
    
    # 2. 카테고리 매핑 및 Row 분리
    df_final = map_and_explode_categories(df_filtered)

    # LECTURE 테이블의 최종 컬럼 순서 조정
    final_columns = [
        'APLY_URL', 'LCTR_WAY', 'LCTR_CATEGORY', 'LCTR_NM', 'APLY_WAY', 
        'APLY_BGN', 'APLY_END', 'LCTR_BGN', 'LCTR_END', 'LCTR_CONTENT', 
        'PCSP', 'IS_APLY_AVL', 'DL_NM', 'ADDRESS', 'LC_1', 'LC_2', 'LC_3', 
        'ADDRESS_X', 'ADDRESS_Y', 'LCTR_IMAGE'
    ]
    return df_final[final_columns]


# def process_digi(df_raw: pd.DataFrame) -> pd.DataFrame:
#     """
#     디지털 배움터 (수강신청 가능) 데이터를 LECTURE 테이블 형식에 맞게 변환 및 파싱.
#     """
#     if df_raw.empty:
#         return pd.DataFrame()
    
#     return pd.DataFrame()


def process_digi_end(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    디지털 배움터 수강 종료 데이터를 LECTURE 테이블 스키마에 맞게 변환한다.

    원본 DataFrame을 기반으로 공통 필터링 및 카테고리 매핑 로직을 적용하여 
    최종 LECTURE 형식의 DataFrame을 반환한다.

    본 데이터는 수강 종료 강좌이므로 신청 관련 컬럼은 고정값으로 처리된다.

    Parameters
    ----------
    df_raw : pandas.DataFrame
        디지털 배움터 수강 종료 강좌 원본 데이터프레임.

    Returns
    -------
    pandas.DataFrame
        LECTURE 테이블 컬럼 구조에 맞게 변환된 데이터프레임.
        입력 데이터가 비어 있는 경우 빈 DataFrame을 반환한다.

    Raises
    ------
    KeyError
        필수 컬럼인 `LCTR_NM`이 입력 데이터프레임에 존재하지 않으면
        공통 필터링 또는 카테고리 매핑 과정에서 오류가 발생할 수 있다.
    """
    if df_raw.empty:
        return pd.DataFrame()

    # [1단계] Raw 데이터 파싱 (LECTURE 스키마의 임시 DataFrame 구성)
    temp_df = pd.DataFrame(index=df_raw.index)

    # 1. APLY_URL
    temp_df['APLY_URL'] = df_raw['APLY_URL']

    # 2. LCTR_WAY (강의 방식) 매핑
    def map_lctr_way(way):
        way = str(way).strip()
        if way in ['집합', '에듀버스']:
            return '오프라인'
        elif way in ['온라인']:
            return '온라인'
        return None 
        
    temp_df['LCTR_WAY'] = df_raw['LCTR_WAY'].apply(map_lctr_way)

    # 3. LCTR_CATEGORY (초기 NULL 처리 - 공통 함수에서 채워짐)
    temp_df['LCTR_CATEGORY'] = None
    
    # 4. LCTR_NM
    temp_df['LCTR_NM'] = df_raw['LCTR_NM']

    # 5. APLY_WAY: '온라인' 고정
    temp_df['APLY_WAY'] = '온라인'

    # 6. APLY_BGN: NULL 고정
    temp_df['APLY_BGN'] = None

    # 7. APLY_END: NULL 고정
    temp_df['APLY_END'] = None

    # 8, 9. LCTR_BGN, LCTR_END 파싱 (예시: '2025-11-17 ~ 2025-11-17 (1 일)')
    
    # ' ~ '를 기준으로 앞 부분(시작일) 추출
    lctr_bgn_series = df_raw['LCTR_BGN_END'].astype(str).str.split('~', expand=True)[0].str.strip()
    # ' ~ '를 기준으로 뒷 부분, 그리고 그 부분에서 ' (' 앞 부분(종료일) 추출
    lctr_end_series_temp = df_raw['LCTR_BGN_END'].astype(str).str.split('~', expand=True)[1].str.strip()
    lctr_end_series = lctr_end_series_temp.str.split('(', expand=True)[0].str.strip()

    temp_df['LCTR_BGN'] = pd.to_datetime(lctr_bgn_series, errors='coerce')
    temp_df['LCTR_END'] = pd.to_datetime(lctr_end_series, errors='coerce')
    
    # 10. LCTR_CONTENT
    temp_df['LCTR_CONTENT'] = df_raw['LCTR_CONTENT']

    # 11. PCSP: NONE 고정
    temp_df['PCSP'] = None

    # 12. IS_APLY_AVL: FALSE 고정
    temp_df['IS_APLY_AVL'] = False

    # 13, 14 DL_NM, ADDRESS 추출
    parsed = df_raw['DL_NM'].apply(parse_dl_nm_and_address)

    temp_df['ADDRESS'] = parsed.apply(lambda x: x[0])
    temp_df['DL_NM'] = parsed.apply(lambda x: x[1])
    
    # 15. NULL 값 지정 컬럼
    temp_df['LC_1'] = None
    temp_df['LC_2'] = None
    temp_df['LC_3'] = None
    temp_df['ADDRESS_X'] = None
    temp_df['ADDRESS_Y'] = None
    temp_df['LCTR_IMAGE'] = None
    
    # [2단계] 공통 필터링 및 카테고리/ROW 분리 로직
    # 1. 강의 필터링
    df_filtered = filter_data(temp_df)
    
    # 2. 카테고리 매핑 및 Row 분리
    df_final = map_and_explode_categories(df_filtered)

    # LECTURE 테이블의 최종 컬럼 순서 조정
    final_columns = [
        'APLY_URL', 'LCTR_WAY', 'LCTR_CATEGORY', 'LCTR_NM', 'APLY_WAY', 
        'APLY_BGN', 'APLY_END', 'LCTR_BGN', 'LCTR_END', 'LCTR_CONTENT', 
        'PCSP', 'IS_APLY_AVL', 'DL_NM', 'ADDRESS', 'LC_1', 'LC_2', 'LC_3', 
        'ADDRESS_X', 'ADDRESS_Y', 'LCTR_IMAGE'
    ]
    return df_final[final_columns]