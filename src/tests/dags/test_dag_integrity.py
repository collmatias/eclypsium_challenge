import pytest


def test_dag_loaded(dagbag):
    """Test the etl_MLA which must have 5 tasks and no errors"""
    dag = dagbag.get_dag(dag_id="etl_MLA")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 9