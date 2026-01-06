# Edu-Chatbot

[DAG Documents](https://edu-navigator.github.io/Edu-Chatbot/index.html) 

## 프로젝트 개요
- 주제  
    디지털 취약 계층을 위한 디지털 교육 정보 통합하여, 상대적으로 접근하기 쉬운 카카오톡 채널을 통해 챗봇 구성

- 목표
  1. 정보 접근성 강화: 취약계층에게 친숙한 '카카오톡'을 통해, 거주지(구/동) 근처의 오프라인 교육 정보와 맞춤형 유튜브 강의 영상을 제공.
  2. 데이터 통합: 디지털 배움터, 공공데이터 포털, 유튜브 등 흩어져 있는 교육 콘텐츠를 하나의 파이프라인으로 수집 및 통합.
  3. 인프라 안정성: 지속적인 최신 정보 업데이트를 위해 AWS 기반의 안정적인 자동화 파이프라인(Airflow) 구축.


## 디렉토리 구조 (초안)
Edu-Chatbot/
├── .github/
│   └── workflows/
│       └── cicd.yml              # GitHub Actions CI/CD 파이프라인 설정
│
├── airflow/
│   └── dags/                     # Airflow DAG 및 관련 모듈
│       ├── common/               # 공통 설정 및 상수 (Constants)
│       ├── scripts/              # 주요 로직 실행 스크립트 (Crawling, ETL 등)
│       ├── utils/                # 유틸리티 함수 모음 (S3, Slack, DB 연결 등)
│       ├── digital_learning_*.py # 디지털 배움터 관련 DAG
│       ├── lecture_dag.py        # 강의 정보 수집 DAG
│       ├── suji_dag.py           # 수지구청 크롤링 DAG
│       └── ...
│
├── docker/                       # Docker 이미지 및 설정 파일
│
├── docs/                         # Sphinx 기반 프로젝트 문서화
│   ├── source/                   # 문서 소스 파일 (.rst)
│   └── build/                    # 빌드된 HTML 문서
│
├── tests/                        # 단위 테스트 및 통합 테스트 코드
│
├── docker-compose.ci.yml         # CI 환경용 도커 컴포즈 설정
├── requirements.txt              # Python 의존성 패키지 목록
└── README.md                     # 프로젝트 설명서


## 기술 스택
<img width="740" height="441" alt="스크린샷 2026-01-06 오전 10 03 51" src="https://github.com/user-attachments/assets/67d1a97b-04cb-41cb-97a2-746ebbe6dcd7" />



## 시스템 아키텍쳐
![스크린샷 2026-01-05 오후 5.58.31.png](attachment:c7e34ec6-f561-4c15-8559-840cea042d4c:스크린샷_2026-01-05_오후_5.58.31.png)

## 데이터 파이프 라인
![image.png](attachment:2c161b46-ca18-438b-a63c-ed02ef3ea82c:image.png)

## 서비스 운용 (카카오톡 채널)
![image.png](attachment:5eb6cfbe-0570-4353-8c3e-ea2039fc8a35:image.png)

