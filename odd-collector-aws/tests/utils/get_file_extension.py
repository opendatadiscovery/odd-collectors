import odd_collector_aws.utils


def test_get_file_extension():
    assert odd_collector_aws.utils.get_file_extension("file.csv") == ".csv"
    assert (
        odd_collector_aws.utils.get_file_extension("/path/to/file.csv.gz") == ".csv.gz"
    )
