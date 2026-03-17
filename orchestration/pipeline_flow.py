from prefect import flow, task
from prefect.schedules import CronSchedule
from ingestion.main_ingest import run_pipeline
import subprocess

@task(name="Extract and Load")
def ingest_data():
    run_pipeline()

@task(name="Transform with dbt")
def run_dbt():
    subprocess.run(["dbt", "run"], cwd="dbt", check=True)

# Schedule: runs on the 1st of every month
# To activate: flow.serve(name="monthly-run", cron="0 0 1 * *")
@flow(name="Spain Climate Tourism Pipeline", log_prints=True)
def main_flow():
    print("Starting pipeline...")
    ingest_data()
    run_dbt()
    print("Pipeline completed.")

if __name__ == "__main__":
    main_flow()