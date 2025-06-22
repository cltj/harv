from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count


def detect_skew(df: DataFrame, column: str, skew_threshold: float = 0.5):
    dist = df.groupBy(column).count()
    total_rows = df.count()
    dist_df = dist.orderBy(col("count").desc())

    top = dist_df.limit(1).collect()[0]
    top_value, top_count = top[column], top["count"]
    skew_ratio = top_count / total_rows

    return {
        "column": column,
        "top_value": top_value,
        "top_count": top_count,
        "total_rows": total_rows,
        "skew_ratio": skew_ratio,
        "is_skewed": skew_ratio > skew_threshold,
        "recommendation": (
            "Try salting, partitioning by another column, or AQE" if skew_ratio > skew_threshold else "No skew detected"
        ),
    }
