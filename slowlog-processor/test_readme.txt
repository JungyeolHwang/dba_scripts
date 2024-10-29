1. 모든 테스트 실행 (커버리지 포함):

pytest --cov=src --cov-report=term-missing



2. 특정 테스트 파일만 실행:

# 설정 테스트
pytest test_config.py --cov=src --cov-report=term-missing

# ES 매니저 테스트
pytest test_elasticsearch_manager.py --cov=src --cov-report=term-missing

# 로그 스트림 프로세서 테스트
pytest test_log_stream_processor.py --cov=src --cov-report=term-missing

# 쿼리 정규화 테스트
pytest test_query_normalizer.py --cov=src --cov-report=term-missing



3. unit 테스트만 실행:

pytest -v -m unit --cov=src --cov-report=term-missing


4. integration 테스트만 실행:

pytest -v -m integration --cov=src --cov-report=term-missing



5. 상세한 출력과 함께 실행:

# 기본 상세 출력
pytest -v --cov=src --cov-report=term-missing

# 더 자세한 출력
pytest -vv --cov=src --cov-report=term-missing



6. 특정 테스트 함수만 실행:

pytest test_config.py::TestConfig::test_config_initialization --cov=src --cov-report=term-missing


7.HTML 리포트 생성 버전 (가장 추천):

# HTML + 터미널 리포트 동시 생성
pytest -v --cov=src --cov-report=term-missing --cov-report=html

# unit 테스트만 HTML 리포트
pytest -v -m unit --cov=src --cov-report=html

# integration 테스트만 HTML 리포트
pytest -v -m integration --cov=src --cov-report=html



8. 모든 리포트 형식 한번에 보기 (가장 상세한 버전):

pytest -vv --cov=src --cov-report=term-missing --cov-report=html --cov-report=xml


개발 시에 자주 사용할만한 추천 조합:

# 개발 중 빠른 확인용
pytest -v --cov=src --cov-report=term-missing

# 상세 분석용
pytest -vv --cov=src --cov-report=term-missing --cov-report=html

# CI/CD용
pytest -v --cov=src --cov-report=term-missing --cov-report=html --cov-report=xml --junitxml=test-results.xml

참고:

--cov-report=term-missing: 터미널에서 누락된 라인 번호 표시
--cov-report=html: HTML 형식의 상세 리포트 생성 (htmlcov 디렉토리에 생성)
--cov-report=xml: Cobertura 호환 XML 리포트 생성 (CI/CD에서 유용)
-v, -vv: 상세한 출력 레벨
--junitxml: JUnit 형식의 테스트 결과 파일 생성 (CI/CD에서 유용)





