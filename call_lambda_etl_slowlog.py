import boto3
import json
import logging
from typing import List, Dict, Any

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_all_db_instances() -> List[Dict[str, str]]:
    """
    모든 RDS/Aurora 인스턴스 정보를 반환
    """
    rds_client = boto3.client('rds')
    instances = []
    
    try:
        paginator = rds_client.get_paginator('describe_db_instances')
        logger.info("Starting to fetch all DB instances...")
        
        for page in paginator.paginate():
            for instance in page['DBInstances']:
                # 상세 디버그 로깅
                logger.info(f"\nChecking instance details:")
                logger.info(f"Instance ID: {instance['DBInstanceIdentifier']}")
                logger.info(f"Engine: {instance['Engine']}")
                logger.info(f"Cluster ID: {instance.get('DBClusterIdentifier', 'N/A')}")
                
                # Aurora, MySQL, MariaDB 엔진만 처리
                if instance['Engine'].startswith(('aurora', 'mysql', 'mariadb')):
                    instance_info = {
                        'instance_id': instance['DBInstanceIdentifier'],
                        'engine': instance['Engine'],
                        'instance_class': instance['DBInstanceClass']
                    }
                    
                    if 'DBClusterIdentifier' in instance:
                        instance_info['cluster_id'] = instance['DBClusterIdentifier']
                    
                    instances.append(instance_info)
                    logger.info(f"Added instance to process: {json.dumps(instance_info, indent=2)}")
        
        logger.info(f"\nFinal selection summary:")
        logger.info(f"Total instances found: {len(instances)}")
        logger.info(f"Instances to process: {json.dumps(instances, indent=2)}")
        
        return instances
        
    except Exception as e:
        logger.error(f"Error getting DB instances: {str(e)}")
        logger.exception("Detailed error trace:")
        raise

def invoke_slow_log_lambda(instance_info: Dict[str, str], dry_run: bool = True) -> None:
    """
    슬로우 쿼리 처리 Lambda 함수 호출 (dry run 모드 지원)
    """
    try:
        payload = {
            'instance_id': instance_info['instance_id'],
            'es_index_prefix': f"mysql-slowlog"  
        }
        
        logger.info(f"\nPreparing to invoke slow log lambda for instance:")
        logger.info(f"Target Lambda: crawling_mysql_slowlog")
        logger.info(f"Payload to be sent: {json.dumps(payload, indent=2)}")
        
        if not dry_run:
            lambda_client = boto3.client('lambda')
            response = lambda_client.invoke(
                FunctionName='crawling_mysql_slowlog',
                InvocationType='Event',
                Payload=json.dumps(payload)
            )
            logger.info(f"Lambda invoked successfully: {response}")
        else:
            logger.info("DRY RUN MODE - Lambda not actually invoked")
        
    except Exception as e:
        logger.error(f"Error preparing/invoking lambda for instance {instance_info['instance_id']}: {str(e)}")
        logger.exception("Detailed error trace:")

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    EventBridge에서 실행되는 메인 핸들러
    """
    try:
        logger.info("\n=== Starting RDS/Aurora Slow Log Processor ===")
        logger.info(f"Event received: {json.dumps(event, indent=2)}")
        
        # dry_run 모드 확인 (기본값: False)
        dry_run = event.get('dry_run', False)
        logger.info(f"Operating in {'DRY RUN' if dry_run else 'PRODUCTION'} mode")
        
        # 모든 대상 인스턴스 목록 가져오기
        db_instances = get_all_db_instances()
        
        if not db_instances:
            logger.warning("No DB instances found. Please check the following:")
            logger.warning("1. Verify that DB instances exist in the RDS console")
            logger.warning("2. Confirm the engine types match the expected values (aurora/mysql/mariadb)")
            logger.warning("3. Verify IAM permissions for describing DB instances")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No DB instances found to process',
                    'instances': []
                })
            }
        
        # 모든 인스턴스에 대해 Lambda 실행
        for instance in db_instances:
            invoke_slow_log_lambda(instance, dry_run)
            
        summary = {
            'mode': 'DRY RUN' if dry_run else 'PRODUCTION',
            'instances_processed': len(db_instances),
            'processed_instances': db_instances
        }
        
        logger.info("\n=== Execution Summary ===")
        logger.info(json.dumps(summary, indent=2))
            
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully processed {len(db_instances)} DB instances',
                'dry_run': dry_run,
                'summary': summary
            })
        }
        
    except Exception as e:
        logger.error(f"Handler error: {str(e)}")
        logger.exception("Detailed error trace:")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Error processing DB instances'
            })
        }
