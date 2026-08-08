import textwrap

from app.lineage.pyspark_parser import parse_pyspark_lineage


def test_read_join_transform_write():
    code = textwrap.dedent(
        """
        from pyspark.sql import functions as F

        df = spark.table("sales.customers")
        orders = spark.read.format("delta").load("staging.orders")
        joined = df.join(orders, "customer_id")
        result = joined.withColumn("total", F.col("amount") * 2).withColumnRenamed("customer_id", "cust_id")
        result.write.format("delta").mode("overwrite").saveAsTable("analytics.customer_totals")
        """
    )
    results = parse_pyspark_lineage(code)

    assert len(results) == 1
    result = results[0]
    assert result.statement_type == "PYSPARK"
    assert result.target_table == "analytics.customer_totals"
    assert result.source_tables == ["sales.customers", "staging.orders"]

    by_target = {c.target_column: c for c in result.column_lineage}
    assert by_target["total"].source_column == "amount"
    assert by_target["cust_id"].source_column == "customer_id"


def test_single_source_table_attribution_survives_transforms():
    code = textwrap.dedent(
        """
        df = spark.table("sales.customers")
        renamed = df.withColumnRenamed("id", "customer_id")
        renamed.write.saveAsTable("analytics.customers_clean")
        """
    )
    result = parse_pyspark_lineage(code)[0]
    mapping = result.column_lineage[0]
    assert mapping.source_table == "sales.customers"
    assert mapping.source_column == "id"
    assert mapping.target_column == "customer_id"


def test_multiple_writes_in_one_script():
    code = textwrap.dedent(
        """
        base = spark.table("sales.orders")
        base.write.saveAsTable("bronze.orders")
        enriched = base.withColumn("amount_usd", col("amount"))
        enriched.write.mode("overwrite").saveAsTable("silver.orders")
        """
    )
    results = parse_pyspark_lineage(code)
    targets = [r.target_table for r in results]
    assert targets == ["bronze.orders", "silver.orders"]


def test_select_with_no_write_produces_no_results():
    code = 'df = spark.table("sales.customers")\nselected = df.select("customer_id", "name")\n'
    assert parse_pyspark_lineage(code) == []
