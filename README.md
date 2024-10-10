DBA 업무를 위한 스크립트 
 - backup_manager.py : 여러 인스턴스 한번에 백업하여 다른 EC2 저장소에 원격 백업하는 스크립트
 - daily_backup_for_table.py : 테이블별로 백업 후 3일간 보관하는 스크립트
 - get_note_from_intranet.py : MYSQL 쪽지 데이터를 읽어서 api gateway를 이용 람다로 DyanamoDB에 저장하는 스크립트
