DELETE FROM airflow.dag_runs WHERE dt = '{{ds}}' AND dag_id = '{{dag.dag_id}}';
INSERT INTO airflow.dag_runs(dt, dag_id)
VALUES ('{{ds}}', '{{dag.dag_id}}');