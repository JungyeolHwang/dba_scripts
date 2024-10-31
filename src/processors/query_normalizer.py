"""SQL 쿼리 정규화를 담당하는 모듈

이 모듈은 SQL 쿼리를 정규화하고 해시를 생성하는 기능을 제공합니다.
정규화된 쿼리는 캐시되어 성능을 향상시킵니다.
""" 

import re
import hashlib
import logging
from typing import Dict, Pattern, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

@dataclass
class QueryNormalizer:
    """SQL 쿼리 정규화 클래스
    
    이 클래스는 SQL 쿼리를 정규화하고 해시를 생성하는 기능을 제공합니다.
    정규화된 쿼리와 해시는 캐시되어 재사용됩니다.
    
    Attributes:
        _patterns (Dict[str, Pattern]): 정규화에 사용되는 정규표현식 패턴들
        _keywords (set[str]): SQL 키워드 집합
        _query_cache (Dict[str, str]): 정규화된 쿼리 캐시
        _hash_cache (Dict[str, str]): 쿼리 해시 캐시
        cache_hits (int): 캐시 히트 수
        cache_misses (int): 캐시 미스 수
    """
    
    _patterns: Dict[str, Pattern] = field(default_factory=dict)
    _keywords: set = field(default_factory=set)
    _query_cache: Dict[str, str] = field(default_factory=dict)
    _hash_cache: Dict[str, str] = field(default_factory=dict)
    cache_hits: int = 0
    cache_misses: int = 0
    
    def __post_init__(self):
        """초기화 메서드: 정규표현식 패턴과 SQL 키워드를 초기화합니다."""
        self._init_patterns()
        self._init_keywords()
    
    def _init_patterns(self) -> None:
        """정규화에 사용할 정규표현식 패턴들을 초기화합니다."""
        self._patterns = {
            'whitespace': re.compile(r'\s+'),
            'comments': re.compile(r'/\*.*?\*/|#[^\n]*\n'),
            'semicolon': re.compile(r';+$'),
            'date_format': re.compile(r"DATE_FORMAT\s*\([^,]+,\s*'[^']*'\)", re.IGNORECASE),
            'numbers': re.compile(r'\b\d+\b'),
            'quoted_strings': re.compile(r'"[^"]*"|\'[^\']*\''),
            'in_clause': re.compile(r'IN\s*\([^)]+\)', re.IGNORECASE),
            'table_aliases': re.compile(r'\b(?:AS\s+)?(?:[`"][\w\s]+[`"]|[\w$]+)\b', re.IGNORECASE),
            'column_aliases': re.compile(r'\b(?:[`"][\w\s]+[`"]|[\w$]+)\.[\w$]+\b', re.IGNORECASE),
            'use_statement': re.compile(r'(?i)^use\s+[\w_]+\s*;?\s*'),
            'set_timestamp': re.compile(r'SET\s+timestamp\s*=\s*\d+\s*;?\s*', re.IGNORECASE),
            'meta_info': re.compile(r'--\s*(?:REMOTE_ADDR|URL|Host)\s*:[^;]*?(?=\s*--|\s*[Ss][Ee][Ll][Ee][Cc][Tt]|\s*$)', 
                                  re.IGNORECASE | re.DOTALL)
        }
    
    def _init_keywords(self) -> None:
        """SQL 키워드를 초기화합니다."""
        self._keywords = {
            'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WHERE', 
            'FROM', 'JOIN', 'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT'
        }
        keywords_pattern = '|'.join(sorted(self._keywords, key=len, reverse=True))
        self._patterns['keywords'] = re.compile(
            r'\b({0})\b'.format(keywords_pattern), 
            re.IGNORECASE
        )

    def normalize_query(self, query: str) -> str:
        """SQL 쿼리를 정규화합니다.
        
        Args:
            query (str): 정규화할 SQL 쿼리
            
        Returns:
            str: 정규화된 SQL 쿼리
            
        Example:
            >>> normalizer = QueryNormalizer()
            >>> query = "SELECT * FROM users WHERE id = 123"
            >>> normalizer.normalize_query(query)
            'SELECT * FROM users WHERE id = ?'
        """
        if not query:
            logger.warning("빈 쿼리가 입력되었습니다.")
            return ""
            
        # 캐시 확인
        if query in self._query_cache:
            self.cache_hits += 1
            logger.debug("쿼리 캐시 히트")
            return self._query_cache[query]
            
        self.cache_misses += 1
        logger.debug("쿼리 캐시 미스, 정규화 수행")
        
        try:
            normalized = self._normalize_query_impl(query)
            
            # 캐시 크기 제한
            if len(self._query_cache) <= 10000:
                self._query_cache[query] = normalized
                
            logger.debug(f"정규화 완료: {query} -> {normalized}")
            return normalized
            
        except Exception as e:
            logger.error(f"쿼리 정규화 중 오류 발생: {str(e)}")
            return query

    def _normalize_query_impl(self, query: str) -> str:
        """쿼리 정규화 구현
        
        Args:
            query (str): 정규화할 SQL 쿼리
            
        Returns:
            str: 정규화된 SQL 쿼리
        """
        logger.debug("정규화 시작")
        
        # 1. 메타 정보 제거
        query = self._patterns['set_timestamp'].sub('', query)
        query = self._patterns['use_statement'].sub('', query)
        query = self._patterns['meta_info'].sub('', query)
        
        # 2. 세미콜론으로 분리하여 마지막 유효 SQL 추출
        query_parts = [part.strip() for part in query.split(';') if part.strip()]
        
        actual_query = ''
        for part in reversed(query_parts):
            part = part.strip()
            if any(keyword in part.upper() for keyword in self._keywords):
                actual_query = part
                break
                
        if not actual_query:
            logger.warning("유효한 SQL을 찾지 못함")
            return ""
            
        # 3. 정규화 적용
        normalized = actual_query
        
        # 공백 정규화
        normalized = self._patterns['whitespace'].sub(' ', normalized)
        
        # 숫자를 ? 로 변경
        normalized = self._patterns['numbers'].sub('?', normalized)
        
        # 문자열을 ? 로 변경
        normalized = self._patterns['quoted_strings'].sub('?', normalized)
        
        # IN 절 정규화
        normalized = self._normalize_in_clauses(normalized)
        
        return normalized.strip()

    def _normalize_in_clauses(self, query: str) -> str:
        """IN 절을 정규화합니다.
        
        Args:
            query (str): 정규화할 쿼리
            
        Returns:
            str: IN 절이 정규화된 쿼리
        """
        # 닫는 괄호가 없는 IN 절 처리
        query = re.sub(r'IN\s*\([^)]*$', 'IN (...)', query, flags=re.IGNORECASE)
        # 일반적인 IN 절 처리
        query = re.sub(r'IN\s*\([^)]+\)', 'IN (...)', query, flags=re.IGNORECASE)
        return query

    def generate_hash(self, query: str) -> str:
        """쿼리의 해시값을 생성합니다.
        
        Args:
            query (str): 해시를 생성할 쿼리
            
        Returns:
            str: 쿼리의 MD5 해시값
            
        Example:
            >>> normalizer = QueryNormalizer()
            >>> query = "SELECT * FROM users WHERE id = 123"
            >>> normalizer.generate_hash(query)
            'a1b2c3d4...'
        """
        if not query:
            logger.warning("빈 쿼리의 해시가 요청됨")
            return hashlib.md5(b'').hexdigest()
            
        # 캐시 확인
        if query in self._hash_cache:
            return self._hash_cache[query]
            
        try:
            normalized_query = self.normalize_query(query)
            query_hash = hashlib.md5(normalized_query.encode()).hexdigest()
            
            # 캐시 크기 제한
            if len(self._hash_cache) <= 10000:
                self._hash_cache[query] = query_hash
                
            return query_hash
            
        except Exception as e:
            logger.error(f"해시 생성 중 오류 발생: {str(e)}")
            return hashlib.md5(query.encode()).hexdigest()

    def get_cache_stats(self) -> Dict[str, int]:
        """캐시 통계를 반환합니다.
        
        Returns:
            Dict[str, int]: 캐시 히트 수와 미스 수를 포함한 통계
        """
        return {
            'cache_hits': self.cache_hits,
            'cache_misses': self.cache_misses,
            'query_cache_size': len(self._query_cache),
            'hash_cache_size': len(self._hash_cache)
        }

    def clear_caches(self) -> None:
        """캐시를 초기화합니다."""
        self._query_cache.clear()
        self._hash_cache.clear()
        self.cache_hits = 0
        self.cache_misses = 0