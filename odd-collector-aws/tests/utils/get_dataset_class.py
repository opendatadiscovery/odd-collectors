from odd_collector_aws.adapters.s3.domain.dataset import (
    CSVS3Dataset,
    ParquetS3Dataset,
    TSVS3Dataset,
    get_dataset_class,
)


def test_get_dataset_class():
    assert get_dataset_class("file.with.dots.in.name.csv") == CSVS3Dataset
    assert get_dataset_class("file.with.dots.in.name.csv.gz") == CSVS3Dataset
    assert get_dataset_class("file.with.dots.in.name.csv.bz2") == CSVS3Dataset
    assert (
        get_dataset_class("very/long/path/to/file.with.dots.in.name.csv")
        == CSVS3Dataset
    )
    assert (
        get_dataset_class("very/long/path/to/file.with.dots.in.name.csv.gz")
        == CSVS3Dataset
    )
    assert (
        get_dataset_class("very/long/path/to/file.with.dots.in.name.csv.bz2")
        == CSVS3Dataset
    )
    assert get_dataset_class("file.with.dots.in.name.tsv") == TSVS3Dataset
    assert get_dataset_class("file.with.dots.in.name.tsv.gz") == TSVS3Dataset
    assert get_dataset_class("file.with.dots.in.name.tsv.bz2") == TSVS3Dataset
    assert (
        get_dataset_class("very/long/path/to/file.with.dots.in.name.tsv")
        == TSVS3Dataset
    )
    assert (
        get_dataset_class("very/long/path/to/file.with.dots.in.name.tsv.gz")
        == TSVS3Dataset
    )
    assert (
        get_dataset_class("very/long/path/to/file.with.dots.in.name.tsv.bz2")
        == TSVS3Dataset
    )
    assert get_dataset_class("file.with.dots.in.name.parquet") == ParquetS3Dataset
    assert (
        get_dataset_class("file.with.dots.in.name.snappy.parquet") == ParquetS3Dataset
    )
    assert (
        get_dataset_class("very/long/path/to/file.with.dots.in.name.parquet")
        == ParquetS3Dataset
    )
    assert (
        get_dataset_class("very/long/path/to/file.with.dots.in.name.snappy.parquet")
        == ParquetS3Dataset
    )
