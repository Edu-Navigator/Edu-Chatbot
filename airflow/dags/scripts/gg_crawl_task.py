from airflow.decorators import task
import pandas as pd
import requests
import math
from airflow.models import Variable

@task
def gg_crawl_task():
    """
    경기도 평생학습 강좌 정보를 OpenAPI를 통해 수집하여 CSV 파일로 저장한다.

    경기도 평생학습 OpenAPI를 호출하여
    전체 강좌 데이터를 페이지 단위로 수집한 뒤,
    정규화된 컬럼 구조로 변환하여 CSV 파일로 저장하고, 저장 경로를 XCom으로 반환한다.

    Returns
    -------
    str
        수집 결과가 저장된 CSV 파일의 전체 경로이다.
        downstream task에서 입력 경로로 사용된다.

    Notes
    -----
    - 데이터 출처
        - 경기도 평생학습 OpenAPI
    - 수집 방식
        - OpenAPI 호출을 통한 JSON 데이터 수집이다.
        - 페이지네이션(pindex, pSize) 기반 전체 데이터 순회이다.
    - 인증 방식
        - Airflow Variable에 저장된 API Key(`gg_api_key`)를 사용한다.
    - 데이터 처리
        - 전체 데이터 건수(list_total_count)를 조회하여 총 페이지 수를 계산한다.
        - 각 페이지 응답의 row 데이터를 누적 수집한다.
        - 필요한 컬럼만 선별하여 DataFrame을 구성한다.
        - 내부 표준 컬럼 스키마에 맞게 DataFrame을 구성한다.
    - 데이터가 없는 경우
        - 빈 DataFrame을 생성하되 컬럼 구조는 유지한다.
    - 결과 저장
        - Airflow Variable `DATA_DIR` 경로 하위에
        `gg_api_res.csv` 파일로 저장한다.
    - 반환값
        - CSV 파일 경로(str)를 반환한다.
        - downstream task에서 입력 경로로 사용된다.
    """

    API_KEY = Variable.get("gg_api_key")
    BASE_URL = "https://openapi.gg.go.kr/ALifetimeLearningLecture"
    PSIZE = 1000

    all_rows = []

    try:
        # 1) 전체 건수 확인
        first_url = f"{BASE_URL}?KEY={API_KEY}&Type=json&pindex=1&pSize={PSIZE}"
        first_res = requests.get(first_url, timeout=10).json()

        if (
            "ALifetimeLearningLecture" not in first_res
            or len(first_res["ALifetimeLearningLecture"]) < 2
        ):
            total_count = 0
        else:
            total_count = first_res["ALifetimeLearningLecture"][0]["head"][0]["list_total_count"]

        total_pages = math.ceil(total_count / PSIZE) if total_count > 0 else 0

        print(f"경기도 평생학습 총 데이터 수: {total_count}")
        print(f"경기도 평생학습 총 페이지 수: {total_pages}")

        # 2) 전체 페이지 반복 수집
        for page in range(1, total_pages + 1):
            url = f"{BASE_URL}?KEY={API_KEY}&Type=json&pindex={page}&pSize={PSIZE}"
            res = requests.get(url, timeout=10).json()

            if (
                len(res.get("ALifetimeLearningLecture", [])) > 1
                and "row" in res["ALifetimeLearningLecture"][1]
            ):
                rows = res["ALifetimeLearningLecture"][1]["row"]
                all_rows.extend(rows)

    except Exception as e:
        raise RuntimeError(f"경기도 평생학습 API 수집 중 오류 발생: {e}")

    # 3) DataFrame 정리
    use_cols = [
        "LECT_TITLE", "EDU_BEGIN_DE", "EDU_END_DE", "EDU_BEGIN_TM", "EDU_END_TM",
        "LECT_CONT", "EDU_TARGET_DIV_NM", "EDU_METH_DIV_NM", "OPERT_WDAY_INFO",
        "EDU_PLC", "LECT_PSN_CNT", "ATENLECT_CHRG", "REFINE_ROADNM_ADDR",
        "OPERT_INST_NM", "OPERT_INST_TELNO", "RECEPT_BEGIN_DE", "RECEPT_END_DE",
        "RECEPT_METH_DIV_NM", "SELECT_METH_DIV_NM", "HMPG_ADDR",
        "OADT_SPORT_LECT_YN", "ACBS_EVALTN_PNT_RECOGN_YN",
        "LFLERNACCT_EVALTN_RECOGN_YN", "DATA_STD_DE"
    ]

    if not all_rows:
        df = pd.DataFrame(columns=use_cols)
    else:
        df = pd.DataFrame(all_rows)[use_cols]

    rename_map = {
        "LECT_TITLE": "LCTR_NM",
        "EDU_BEGIN_DE": "LCTR_BGN",
        "EDU_END_DE": "LCTR_END",
        "EDU_BEGIN_TM": "LCTR_BGN_TIME",
        "EDU_END_TM": "LCTR_END_TIME",
        "LECT_CONT": "LCTR_CONTENT",
        "EDU_TARGET_DIV_NM": "LCTR_TARGET",
        "EDU_METH_DIV_NM": "LCTR_WAY",
        "OPERT_WDAY_INFO": "LCTR_DAY",
        "EDU_PLC": "LCTR_LOCATE",
        "LECT_PSN_CNT": "LCTR_PCSP",
        "ATENLECT_CHRG": "LCTR_PRICE",
        "REFINE_ROADNM_ADDR": "LCTR_ADDRESS",
        "OPERT_INST_NM": "LCTR_OPERATE",
        "OPERT_INST_TELNO": "OPERATE_TELEPHONE",
        "RECEPT_BEGIN_DE": "APLY_BGN",
        "RECEPT_END_DE": "APLY_END",
        "RECEPT_METH_DIV_NM": "APLY_WAY",
        "SELECT_METH_DIV_NM": "SELECT_WAY",
        "HMPG_ADDR": "APLY_URL",
        "OADT_SPORT_LECT_YN": "NOTUSE1",
        "ACBS_EVALTN_PNT_RECOGN_YN": "NOTUSE2",
        "LFLERNACCT_EVALTN_RECOGN_YN": "NOTUSE3",
        "DATA_STD_DE": "NOTUSE4"
    }

    df = df.rename(columns=rename_map)

    df["LCTR_PCSP"] = pd.to_numeric(df["LCTR_PCSP"], errors="coerce").astype("Int64")
    df["LCTR_PRICE"] = pd.to_numeric(df["LCTR_PRICE"], errors="coerce").astype("Int64")

    path = f"{Variable.get('DATA_DIR')}/gg_api_res.csv"
    df.to_csv(path, index=False)

    # XCom 반환
    return path
