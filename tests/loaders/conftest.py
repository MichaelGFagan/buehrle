import pytest

import dlt


@pytest.fixture
def fake_make_pipeline(tmp_path):
    """Replacement for loaders.dlt_utils.make_pipeline that targets tmp_path."""
    def factory(name):
        return dlt.pipeline(
            pipeline_name=name + '_main_test',
            destination=dlt.destinations.duckdb(str(tmp_path / 'test.duckdb')),
            dataset_name=name,
            pipelines_dir=str(tmp_path / 'dlt'),
        )
    return factory
