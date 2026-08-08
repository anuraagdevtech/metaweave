from app.lineage.sql_parser import UnsupportedStatementError, parse_sql_lineage, parse_sql_script


def test_insert_select_column_lineage():
    sql = """
    INSERT INTO analytics.customer_summary (customer_id, total_orders, total_revenue)
    SELECT c.customer_id, COUNT(o.order_id) AS total_orders, SUM(o.amount) AS total_revenue
    FROM sales.customers c
    JOIN sales.orders o ON o.customer_id = c.customer_id
    GROUP BY c.customer_id
    """
    result = parse_sql_lineage(sql)

    assert result.statement_type == "INSERT"
    assert result.target_table == "analytics.customer_summary"
    assert result.source_tables == ["sales.customers", "sales.orders"]

    by_target = {c.target_column: c for c in result.column_lineage}
    assert by_target["customer_id"].source_table == "sales.customers"
    assert by_target["customer_id"].source_column == "customer_id"
    assert by_target["total_orders"].source_table == "sales.orders"
    assert by_target["total_orders"].source_column == "order_id"
    assert by_target["total_revenue"].source_column == "amount"


def test_merge_column_lineage_update_and_insert_branches():
    sql = """
    MERGE INTO analytics.customer_dim AS tgt
    USING staging.customer_stage AS src
    ON tgt.customer_id = src.customer_id
    WHEN MATCHED THEN UPDATE SET tgt.name = src.name, tgt.email = src.email
    WHEN NOT MATCHED THEN INSERT (customer_id, name, email) VALUES (src.customer_id, src.name, src.email)
    """
    result = parse_sql_lineage(sql)

    assert result.statement_type == "MERGE"
    assert result.target_table == "analytics.customer_dim"
    assert result.source_tables == ["staging.customer_stage"]

    update_mappings = [c for c in result.column_lineage if c.transformation.startswith("src.")]
    targets = {c.target_column for c in update_mappings}
    assert {"name", "email", "customer_id"} <= targets
    for mapping in update_mappings:
        assert mapping.source_table == "staging.customer_stage"


def test_create_table_as_select():
    sql = """
    CREATE TABLE analytics.active_customers AS
    SELECT c.customer_id, c.name, c.email
    FROM sales.customers c
    WHERE c.status = 'ACTIVE'
    """
    result = parse_sql_lineage(sql)

    assert result.statement_type == "CREATE_TABLE_AS"
    assert result.target_table == "analytics.active_customers"
    assert result.source_tables == ["sales.customers"]
    assert len(result.column_lineage) == 3
    assert all(c.source_table == "sales.customers" for c in result.column_lineage)


def test_plain_select_has_no_target_table():
    result = parse_sql_lineage("SELECT o.order_id, o.amount FROM sales.orders o")
    assert result.statement_type == "SELECT"
    assert result.target_table is None
    assert result.source_tables == ["sales.orders"]


def test_literal_projection_has_no_source_column():
    result = parse_sql_lineage("INSERT INTO t (load_type) SELECT 'batch' AS load_type")
    mapping = result.column_lineage[0]
    assert mapping.source_table is None
    assert mapping.source_column is None


def test_unsupported_statement_raises():
    import pytest

    with pytest.raises(UnsupportedStatementError):
        parse_sql_lineage("DELETE FROM sales.orders WHERE order_id = 1")


def test_parse_sql_script_multi_statement():
    script = """
    INSERT INTO a.t1 SELECT x FROM b.src1;
    CREATE TABLE a.t2 AS SELECT y FROM b.src2;
    """
    results = parse_sql_script(script)
    assert [r.statement_type for r in results] == ["INSERT", "CREATE_TABLE_AS"]
    assert results[0].target_table == "a.t1"
    assert results[1].target_table == "a.t2"
