from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 region = "us-west-2",
                 json_path = "auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table              = table
        self.s3_bucket          = s3_bucket
        self.s3_key             = s3_key
        self.json_path          = json_path
        self.region             = region

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        self.log.info(f'aws creddentials id: {self.aws_credentials_id}')
        credentials = aws_hook.get_credentials()
        #create redshift connection
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        #truncate table
        self.log.info(f"Truncating table: {self.table}")
        redshift_hook.run(f"TRUNCATE TABLE {self.table}")
        #copy staging s3 bucket to target table
        self.log.info(f"Copying FROM S3 BUCKET to redshift table: {self.table}")
        sql = f"""
              COPY {self.table} FROM 's3://{self.s3_bucket}/{self.s3_key}' 
              ACCESS_KEY_ID '{credentials.access_key}'
              SECRET_ACCESS_KEY '{credentials.secret_key}'
              COMPUPDATE off
              REGION '{self.region}'
              FORMAT AS JSON '{self.json_path}'
              """
        redshift_hook.run(sql)




