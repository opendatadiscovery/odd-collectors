from odd_collector_aws.adapters.s3.mapper.dataset import s3_path_to_name


def test_s3_path_to_name():
    assert s3_path_to_name("bucket/path/to/file.csv") == "path\\\\to\\\\file.csv"
    assert s3_path_to_name("bucket/path/to/") == "path\\\\to"
