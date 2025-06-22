# detools/analyzer.py
from pyspark.sql import DataFrame


def analyze_dataframe(df: DataFrame, df_name: str = "df") -> dict:
    plan = df._jdf.queryExecution().executedPlan().toString()
    logical = df._jdf.queryExecution().logical().toString()

    return {
        "df_name": df_name,
        "has_python_udf": "PythonUDF" in plan,
        "has_shuffle": "Exchange" in plan,
        "has_cartesian": "CartesianProduct" in plan,
        "join_type": (
            "SortMergeJoin" if "SortMergeJoin" in plan else "BroadcastHashJoin" if "BroadcastHashJoin" in plan else None
        ),
        "logical_plan": logical,
        "physical_plan": plan,
    }
