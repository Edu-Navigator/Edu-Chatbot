[제목 없음 2e1d82b3d7ed80248592f0911f48e217.md](https://github.com/user-attachments/files/24463054/2e1d82b3d7ed80248592f0911f48e217.md)
# 제목 없음

# Edu-Chatbot

[DAG Documents](https://edu-navigator.github.io/Edu-Chatbot/index.html)

## 프로젝트 개요

- 주제
    
    디지털 취약 계층을 위한 디지털 교육 정보 통합하여, 상대적으로 접근하기 쉬운 카카오톡 채널을 통해 챗봇 구성
    
- 목표
    1. 정보 접근성 강화: 취약계층에게 친숙한 '카카오톡'을 통해, 거주지(구/동) 근처의 오프라인 교육 정보와 맞춤형 유튜브 강의 영상을 제공.
    2. 데이터 통합: 디지털 배움터, 공공데이터 포털, 유튜브 등 흩어져 있는 교육 콘텐츠를 하나의 파이프라인으로 수집 및 통합.
    3. 인프라 안정성: 지속적인 최신 정보 업데이트를 위해 AWS 기반의 안정적인 자동화 파이프라인(Airflow) 구축.

## 디렉토리 구조

Edu-Chatbot/
├── .github/
│   └── workflows/                  # GitHub Actions CI/CD 파이프라인
│
├── airflow/
│   └── dags/                       # Airflow DAG 및 관련 모듈
│       ├── common/                 # 공통 설정 및 상수 (Default Args 등)
│       ├── scripts/                # 주요 로직 실행 스크립트 (ETL, Crawling)
│       ├── utils/                  # 유틸리티 함수 모음 (S3, WebDriver 등)
│       └── ...                     # 각종 DAG 파일들 (.py)
│
├── docker/
│   └── Dockerfile                  # Airflow 커스텀 이미지 빌드 설정
│
├── docs/                           # Sphinx 기반 프로젝트 문서화
│   ├── build/                      # 빌드 결과물 저장소 (HTML)
│   ├── source/                     # 문서 소스 파일
│   └── Makefile                    # 문서 빌드 명령어
│
├── tests/                          # 단위 및 통합 테스트 코드
│
├── docker-compose.ci.yml           # CI 환경용 도커 컴포즈 설정
├── requirements.txt                # Python 의존성 패키지 목록
└── README.md                       # 프로젝트 설명서

## 기술 스택

| **분류** | **기술 상세** | **비고** |
| --- | --- | --- |
| **Infrastructure** | AWS (VPC, EC2, S3, RDS, Lambda) | Private/Public Subnet 분리, 고가용성 아키텍처 |
| **Orchestration** | Apache Airflow | Docker Compose 기반, Celery Executor (분산 처리) |
| **Container** | Docker, Docker Compose | Airflow 및 모니터링 도구 컨테이너화 운영 |
| **Database** | PostgreSQL (Meta), AWS RDS (Service) | 용도별 DB 분리 (메타데이터/서비스 데이터) |
| **Monitoring** | Prometheus, Grafana, Node Exporter | 서버 리소스 실시간 시각화 및 장애 감지 |
| **Data Source** | 크롤링(Selenium, BS4), 공공 데이터 포털, 경기 데이터 드림 | 디지털 배움터, 공공데이터 등 수집 |
| **CI/CD** | GitHub Actions | Main 브랜치 푸시 시 자동 배포 파이프라인 구축 |
| **Service** | 카카오 채널 챗봇 | 시나리오 설계 및 사용자 발화 의도 파악 |
| **Document** | Sphinx | Code 문서화 |

## 시스템 아키텍쳐

![스크린샷 2026-01-05 오후 5.58.31.png](%EC%A0%9C%EB%AA%A9%20%EC%97%86%EC%9D%8C/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2026-01-05_%E1%84%8B%E1%85%A9%E1%84%92%E1%85%AE_5.58.31.png)

## 데이터 파이프 라인

![image.png](%EC%A0%9C%EB%AA%A9%20%EC%97%86%EC%9D%8C/image.png)

## 서비스 운용 (카카오톡 채널)

![image.png](%EC%A0%9C%EB%AA%A9%20%EC%97%86%EC%9D%8C/image%201.png)

## CI/CD

![스크린샷 2026-01-06 오전 10.48.18.png](%EC%A0%9C%EB%AA%A9%20%EC%97%86%EC%9D%8C/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2026-01-06_%E1%84%8B%E1%85%A9%E1%84%8C%E1%85%A5%E1%86%AB_10.48.18.png)
