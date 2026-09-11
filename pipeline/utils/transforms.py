import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from typing import List, Tuple

def standardise_strings(df: DataFrame, columns: List[str]) -> DataFrame:
    """Trim whitespace and lowercase the specified string columns."""
    for col in columns:
        df = df.withColumn(col, F.lower(F.trim(F.col(col))))
    return df

def cast_timestamp(df: DataFrame, col_name: str, fmt: str = 'yyyy-MM-dd HH:mm:ss') -> DataFrame:
    """Cast a string column to timestamp using the given format."""
    return df.withColumn(col_name, F.to_timestamp(F.col(col_name), fmt))

def add_surrogate_key(df: DataFrame, key_columns: List[str], key_name: str = 'sk') -> DataFrame:
    """Add a surrogate key column by hashing the concatenated key columns."""
    return df.withColumn(key_name, F.md5(F.concat_ws('|', *[F.col(c) for c in key_columns])))

def add_audit_columns(df: DataFrame) -> DataFrame:
    """Add standard audit columns: _loaded_at and _source."""
    return df.withColumn('_loaded_at', F.current_timestamp()) \
             .withColumn('_source', F.lit('medallion_pipeline'))

def deduplicate(df: DataFrame, partition_cols: List[str], order_col: str, ascending: bool = False) -> DataFrame:
    """Deduplicate a DataFrame using ROW_NUMBER, keeping the first row per partition based on order_col."""
    order_expr = F.col(order_col).asc() if ascending else F.col(order_col).desc()
    window_spec = Window.partitionBy(*partition_cols).orderBy(order_expr)
    
    return df.withColumn('_row_num', F.row_number().over(window_spec)) \
             .filter(F.col('_row_num') == 1) \
             .drop('_row_num')

def null_safe_trim(df: DataFrame, columns: List[str]) -> DataFrame:
    """Trim string columns and convert empty strings to None/null."""
    for col in columns:
        trimmed = F.trim(F.col(col))
        df = df.withColumn(col, F.when(trimmed == '', None).otherwise(trimmed))
    return df

def validate_not_null(df: DataFrame, columns: List[str]) -> Tuple[DataFrame, DataFrame]:
    """
    Separate a DataFrame into valid and invalid records based on null values in required columns.
    Returns (valid_df, invalid_df).
    """
    condition = F.lit(True)
    for col in columns:
        condition = condition & F.col(col).isNotNull()
        
    valid_df = df.filter(condition)
    invalid_df = df.filter(~condition)
    return valid_df, invalid_df
