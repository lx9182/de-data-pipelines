from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.dq_checks = dq_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for dq_check in self.dq_checks:
            expected_result = dq_check['expected_result']
            actual_result = redshift.get_records(dq_check['check_sql'])[0][0]
            if expected_result != actual_result:
                raise ValueError(f"Data quality check failed")
        self.log.info(f"All data quality check pass")
