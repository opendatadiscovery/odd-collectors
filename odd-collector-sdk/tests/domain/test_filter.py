from odd_collector_sdk.domain.filter import Filter


def test_default_filter():
    odd_filter = Filter()
    assert odd_filter.is_allowed("test_table")
    assert odd_filter.is_allowed("test_column")
    assert odd_filter.is_allowed("test_schema")


def test_default_filter_with_excluded():
    odd_filter = Filter(exclude=[".*_pii"])
    assert odd_filter.is_allowed("table_one")
    assert not odd_filter.is_allowed("table_one_pii")


def test_default_filter_with_excluded_2():
    filter = Filter(exclude=["pii"])
    assert filter.is_allowed("table_one")
    assert not filter.is_allowed("table_one_pii")


def test_default_filter_with_included():
    filter = Filter(include=["dev"])
    assert filter.is_allowed("dev_table")
    assert not filter.is_allowed("prod_table")


def test_case_sensitive_include_filter():
    odd_filter = Filter(include=["dev_table"])
    assert odd_filter.is_allowed("dev_table")
    assert not odd_filter.is_allowed("prod_table")


def test_case_sensitive_exclude_filter():
    odd_filter = Filter(include=["test_table_.*"], exclude=[".*_pii"])
    assert odd_filter.is_allowed("test_table_one")
    assert not odd_filter.is_allowed("test_table_pii")


def test_case_insensitive_include_filter():
    odd_filter = Filter(include=["table_", "column_"], ignore_case=True)
    assert odd_filter.is_allowed("Table_one")
    assert odd_filter.is_allowed("COLumn_X")
    assert not odd_filter.is_allowed("column")


def test_case_insensitive_exclude_filter():
    odd_filter = Filter(include=["table_"], exclude=[".*_pii"], ignore_case=True)
    assert odd_filter.is_allowed("table_one")
    assert not odd_filter.is_allowed("table_one_PII")


def test_case_if_not_include():
    default_include_value = [".*"]
    odd_filter = Filter(include=[], exclude=[])
    assert odd_filter.include == default_include_value
