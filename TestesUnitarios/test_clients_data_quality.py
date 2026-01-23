import pytest

from ..DataQuality.clients_data_quality import DataQualityHandler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.errors.exceptions.captured import AnalysisException


spark = (
    SparkSession.builder
    .appName("teste_itau")
    .master("local[*]")
    .getOrCreate()
)

dqh = DataQualityHandler()

def test_check_nulls_happy_path_returns_only_offensors():
    df = spark.createDataFrame(
        [
            ("123", "Leon", "(11)9999-4356"),
            ("321", None, "(11)99459-4356"),
            ("333", "Ashley", None),
            ("444", "Nemesis", "(11)9119-4356"),
        ],
        ["cod_cliente", "nm_cliente", "num_telefone_cliente"],
    )

    output = dqh.check_nulls(df_silver=df, required_cols=["nm_cliente", "num_telefone_cliente"])

    got_results = output.select("cod_cliente", "data_quality_offensor_cat", "analyzed_column", "offensor").collect()
    got_ids = {row["cod_cliente"] for row in got_results}

    assert got_ids == {"321", "333"}
    assert all(row["data_quality_offensor_cat"] == "nulls_required" for row in got_results)
    assert all(row["analyzed_column"] == "nm_cliente,num_telefone_cliente" for row in got_results)
    assert all(row["offensor"] is None for row in got_results)
    assert output.filter(col("created_at").isNull()).count() == 0


def test_check_nulls_edge_all_nulls_returns_all_rows():
    df = spark.createDataFrame(
        [("123", None, None), ("321", None, None)],
        ["cod_cliente", "nm_cliente", "num_telefone_cliente"],
    )
    output = dqh.check_nulls(df_silver=df, required_cols=["nm_cliente", "num_telefone_cliente"])
    assert {row["cod_cliente"] for row in output.select("cod_cliente").collect()} == {"123", "321"}


def test_check_nulls_error_missing_required_column_raises():
    df = spark.createDataFrame(
        [
            ("123", "Leon", "(11)9999-4356"),
            ("321", None, "(11)99459-4356"),
            ("333", "Ashley", None),
            ("444", "Nemesis", "(11)9119-4356"),
        ],
        ["cod_cliente", "nm_cliente", "num_telefone_cliente"],
    )

    with pytest.raises(AnalysisException):
        dqh.check_nulls(df_silver=df, required_cols=["col_que_nao_existe"]).count()
