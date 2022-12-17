import importlib
import os
import glob
from dateutil.parser import parse as parsedate
from datetime import date
import pytest
from collections import namedtuple
from airflow.models import DAG, DagBag, TaskInstance


# Custom data structure to store DAGs
DagSpec = namedtuple("DagSpec", ["dags", "module_name"])

def _specs_in_path(dag_path):
    """
    Get all specs from your DAG object.

    Args:
        dag_path (String): path to your DAG file.

    Returns:
        namedtuple: contains DAG specs and DAG name.
    """
    _, dag_file = os.path.split(dag_path)
    module_name, _ = os.path.splitext(dag_file)

    mod_spec = importlib.util.spec_from_file_location(module_name, dag_path)
    module = importlib.util.module_from_spec(mod_spec)
    mod_spec.loader.exec_module(module)
    values = vars(module).values()

    def with_attributes(dag):
        dag.full_filepath = dag_path
        return dag

    return DagSpec(
        dags=[with_attributes(obj) for obj in values if isinstance(obj, DAG)],
        module_name=module_name,
    )

# Root dir of your DAGs
DAG_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dags"))

# Absolute paths of your DAGs
DAG_PATHS = [
    os.path.join(DAG_ROOT, f)
    for f in os.listdir(DAG_ROOT)
    if os.path.isfile(os.path.join(DAG_ROOT, f))
]

# List containing a namedtuple with DAG with specs and DAG name
DAG_SPECS = [_specs_in_path(dag_path) for dag_path in DAG_PATHS]

# List containing only the DAG specs
DAGS = [dag_spec.dags[0] for dag_spec in DAG_SPECS]


@pytest.fixture()
def dagbag():
    return DagBag(DAG_ROOT)


def test_dags_load(dagbag):
    """
    Test if DAG has import failues.
    """
    assert len(dagbag.import_errors) == 0, "No Import Failures"
    dags_names_list = [dag for dag in dagbag.dags]
    assert len(dags_names_list) == len(DAGS), "All DAGs loaded"


@pytest.mark.parametrize("dag", DAGS, ids=lambda dag: dag.dag_id)
def test_dag_dry_run(dag):
    """
    Test DAG dry run.
    """
    for task in dag.tasks:
        task_instance = TaskInstance(task, parsedate(date.today().isoformat()))
        task_instance.dry_run()


@pytest.mark.parametrize("dag", DAGS, ids=lambda dag: dag.dag_id)
def test_dag_complete(dag):
    """
    Test if last task has consistent name (dag_complete).
    """
    assert "dag_complete" in str(dag.leaves), "Task Complete name is consistent"


@pytest.mark.parametrize("dag", DAGS, ids=lambda dag: dag.dag_id)
def test_dag_tasks_sqls_templates(dag):
    """
    Test if SQL path for DAG is consistent.
    """
    sqls_files_list = [
        file_ for file_ in glob.glob(f"{dag.template_searchpath[0]}*.sql")
    ]
    sqls_list = [os.path.basename(file_) for file_ in sqls_files_list]

    for task in dag.tasks:
        if hasattr(task, "sql") and task.sql[1:].strip() != "":
            assert task.sql[0] == "/", "Query starts with /"
            assert task.sql[1:] in sqls_list, "Task SQL and file are the same"


@pytest.mark.parametrize("dag", DAGS, ids=lambda dag: dag.dag_id)
def test_dag_id_and_filename(dag):
    """
    Test if DAG ID and DAG filename are equal.
    """
    assert dag.dag_id in dag.filepath, "DAG ID and name are the same"

# Dict containing and specific Operator to test and it's task name
task_types_starts_without_slash = {"YourOperator": ["task_name_of_your_operator"]}


@pytest.mark.parametrize("dag", DAGS, ids=lambda dag: dag.dag_id)
def test_dag_starts_without_slash(dag):
    """
    Test if DAG task name variable starts without slash "/"
    """
    for task in dag.tasks:
        if task.task_type in task_types_starts_without_slash:
            dict_items = task.__dict__
            for variable in task_types_starts_without_slash.get(task.task_type):
                assert dict_items[variable][0] != "/", "Path doesn't start with /"


# Dict containing and specific Operator to test and it's task name
task_types_ends_with_slash = {
    "YourOperator": ["output_location"],
    "YourOperator": ["dest_bucket"],
    "YourOperator": ["destination_object"],
}


@pytest.mark.parametrize("dag", DAGS, ids=lambda dag: dag.dag_id)
def test_dag_end_with_slash(dag):
    """
    Test if DAG task name variable ends with slash "/"
    """
    for task in dag.tasks:
        if task.task_type in task_types_ends_with_slash:
            dict_items = task.__dict__
            for variable in task_types_ends_with_slash.get(task.task_type):
                assert dict_items[variable][-1] == "/", "Output path ends with /"
