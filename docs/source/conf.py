import os
import sys

# ==========================================
# [1] "블랙홀" Mock 클래스 (유지)
# ==========================================
class BlackHoleMock(object):
    def __init__(self, *args, **kwargs):
        self.__name__ = 'BlackHoleMock' 
        self.__all__ = [] 
        self.__path__ = []

    def __call__(self, *args, **kwargs):
        return self

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        return self

    def __getitem__(self, name):
        return self
        
    def __setitem__(self, name, value):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __rshift__(self, other): return self
    def __lshift__(self, other): return self
    def __rrshift__(self, other): return self
    def __rlshift__(self, other): return self
    
    def __truediv__(self, other): return self
    def __rtruediv__(self, other): return self
    def __fspath__(self): return "mock/path"
    def __str__(self): return "mock_string"
    def __add__(self, other): return self
    def __radd__(self, other): return self

    def __bool__(self): return True
    def __iter__(self): return iter([])

# ==========================================
# [2] 시스템 모듈 강제 교체 (유지)
# ==========================================
MOCK_MODULES = [
    # Airflow Core
    'airflow', 'airflow.models', 'airflow.models.dag',
    'airflow.decorators',
    'airflow.operators', 'airflow.operators.python', 'airflow.operators.bash',
    'airflow.operators.empty',   # [추가] EmptyOperator 에러 해결
    'airflow.operators.email',   # [추가] EmailOperator 에러 해결
    'airflow.sensors', 'airflow.sensors.external_task',
    'airflow.utils', 'airflow.utils.task_group', 'airflow.utils.dates',
    'airflow.exceptions',        # [추가] AirflowSkipException 에러 해결
    
    # Airflow Providers
    'airflow.providers',
    'airflow.providers.amazon',
    'airflow.providers.amazon.aws',
    'airflow.providers.amazon.aws.hooks',
    'airflow.providers.amazon.aws.hooks.s3',
    'airflow.providers.postgres',
    'airflow.providers.postgres.hooks',
    'airflow.providers.postgres.hooks.postgres',
    
    # External Libs
    'prophet',
    'webdriver_manager', 'webdriver_manager.chrome',
    'selenium', 'selenium.webdriver', 'selenium.webdriver.chrome',
    'selenium.webdriver.chrome.options', 'selenium.webdriver.chrome.service',
    'selenium.webdriver.common.by', 'selenium.webdriver.support',
    'selenium.webdriver.support.ui', 'selenium.webdriver.remote',
    'selenium.webdriver.remote.remote_connection',
]

mock_instance = BlackHoleMock()
for mod_name in MOCK_MODULES:
    sys.modules[mod_name] = mock_instance

# ==========================================
# [3] 경로 설정 (★여기가 핵심 수정됨★)
# ==========================================
# 1. Sphinx가 'dags' 패키지를 찾기 위한 경로 (airflow 폴더)
sys.path.insert(0, os.path.abspath('../../airflow'))

# 2. 실제 코드 내부에서 'from scripts import...'가 작동하기 위한 경로 (dags 폴더)
#    이게 없어서 ModuleNotFoundError: No module named 'scripts' 가 떴던 것입니다.
sys.path.insert(0, os.path.abspath('../../airflow/dags'))

# ==========================================
# [4] 프로젝트 정보 (유지)
# ==========================================
project = 'Digital Edu Chatbot'
copyright = '2025, Team 7'
author = 'Team 7'
release = '1.0'

# ==========================================
# [5] 일반 설정 (유지)
# ==========================================
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx_autodoc_typehints',
]
templates_path = ['_templates']
language = 'ko'

exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# ==========================================
# [6] Napoleon 설정 (유지)
# ==========================================
napoleon_google_docstring = False
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_param = True
napoleon_use_rtype = True

# ==========================================
# [7] HTML 테마 (유지)
# ==========================================
html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']

# ==========================================
# [8] 추가 Mocking (유지)
# ==========================================
autodoc_mock_imports = [
    "pendulum", "requests", "bs4", "kakaotrans",
    "psycopg2", "sqlalchemy", "pandas", "dateutil"
]