import io
import os

import pytest

from odd_collector_sdk.utils.yaml_parser import ParserError, parse_yaml


def test_empty_yaml_parser():
    stream = io.StringIO(
        """
    """
    )
    assert parse_yaml(stream) is None


def test_yaml_parser_with_variables():
    stream = io.StringIO(
        """
        "foo": "bar"
    """
    )
    assert parse_yaml(stream) == {"foo": "bar"}


def test_yaml_parser_when_env_not_found():
    stream = io.StringIO(
        """
        "foo": !ENV ${BAR}
    """
    )
    with pytest.raises(ParserError, match=r"Environment variable BAR not found"):
        parse_yaml(stream)


def test_yaml_parser_with_tags():
    stream = io.StringIO(
        """
        "foo": !ENV ${BAR}
    """
    )
    os.environ["BAR"] = "bar"
    assert parse_yaml(stream) == {"foo": "bar"}
    os.unsetenv("BAR")


def test_yaml_parser_with_many_variables():
    stream = io.StringIO(
        """
        "foo": !ENV ${BAR}
        "bar": "foo"
        "foo2": !ENV ${BAR2}
    """
    )
    os.environ["BAR"] = "bar"
    os.environ["BAR2"] = "bar2"
    assert parse_yaml(stream) == {"foo": "bar", "bar": "foo", "foo2": "bar2"}
    os.unsetenv("BAR")
    os.unsetenv("BAR2")


def test_yaml_parser_with_many_variables_in_one_line():
    stream = io.StringIO(
        """
        "foo": !ENV ${BAR}/${BAR2}/foo/${BAR3}
        "bar": "foo"
        "foo2": !ENV ${BAR2}
    """
    )
    os.environ["BAR"] = "bar"
    os.environ["BAR2"] = "bar2"
    os.environ["BAR3"] = "bar3"
    assert parse_yaml(stream) == {
        "foo": "bar/bar2/foo/bar3",
        "bar": "foo",
        "foo2": "bar2",
    }
    os.unsetenv("BAR")
    os.unsetenv("BAR2")
    os.unsetenv("BAR3")
