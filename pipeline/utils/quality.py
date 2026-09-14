import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from typing import List, Tuple, Dict, Any
import logging

logger = logging.getLogger(__name__)

def check_null_rate(df: DataFrame, column: str, threshold: float = 0.05) -> Tuple[bool, float]:
    """
    Check if the null rate for a column is below the specified threshold.
    Returns (pass_status, actual_rate).
    """
    total_count = df.count()
    if total_count == 0:
        return False, 1.0
        
    null_count = df.filter(F.col(column).isNull()).count()
    actual_rate = null_count / total_count
    return actual_rate <= threshold, actual_rate

def check_unique(df: DataFrame, columns: List[str]) -> Tuple[bool, int]:
    """
    Check if the specified columns form a unique key.
    Returns (pass_status, duplicate_count).
    """
    total_count = df.count()
    distinct_count = df.select(*columns).distinct().count()
    duplicate_count = total_count - distinct_count
    return duplicate_count == 0, duplicate_count

def check_row_count(df: DataFrame, min_rows: int = 1) -> Tuple[bool, int]:
    """
    Check if the DataFrame has at least min_rows.
    Returns (pass_status, count).
    """
    count = df.count()
    return count >= min_rows, count

def check_referential_integrity(df: DataFrame, ref_df: DataFrame, join_col: str) -> Tuple[bool, int]:
    """
    Check if all values in df[join_col] exist in ref_df[join_col] (ignoring nulls in df).
    Returns (pass_status, orphan_count).
    """
    # Find records in df that don't match any record in ref_df
    orphans = df.filter(F.col(join_col).isNotNull()) \
                .join(ref_df, df[join_col] == ref_df[join_col], 'left_anti')
    orphan_count = orphans.count()
    return orphan_count == 0, orphan_count

def run_quality_suite(checks_results: List[Dict[str, Any]], table_name: str) -> None:
    """
    Print a formatted quality report and raise an exception if any critical check fails.
    checks_results should be a list of dictionaries with keys:
    'name': str, 'passed': bool, 'metric': any, 'critical': bool
    """
    logger.info(f"--- Quality Report for {table_name} ---")
    failed_critical = False
    
    for check in checks_results:
        status = "PASS" if check['passed'] else "FAIL"
        critical_tag = "[CRITICAL]" if check.get('critical', False) else "[WARNING]"
        logger.info(f"{status} {critical_tag} - {check['name']}: {check.get('metric')}")
        
        if not check['passed'] and check.get('critical', False):
            failed_critical = True
            
    if failed_critical:
        raise ValueError(f"Critical quality checks failed for table {table_name}")
    logger.info(f"Quality checks completed for {table_name}")
