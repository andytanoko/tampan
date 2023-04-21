import os
from dask.distributed import Client
from kafka import KafkaConsumer
from prefect import Flow, Task, task
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, push_to_gateway
import time

class BatchJob:
    def __init__(self):
        # Create registry
        self.registry = CollectorRegistry()

        # Define metrics
        labels = ['service', 'task']
        self.job_duration = Histogram('batch_job_duration_seconds', 'Duration of batch job in seconds', labels=labels, registry=self.registry)
        self.job_success = Counter('batch_job_success_total', 'Total number of successful batch job runs', labels=labels[:-1], registry=self.registry)
        self.job_failure = Counter('batch_job_failure_total', 'Total number of failed batch job runs', labels=labels[:-1], registry=self.registry)
        self.job_progress = Gauge('batch_job_progress', 'Number of items processed in current batch', labels=labels[:-1], registry=self.registry)
        self.job_task_success = Counter('batch_job_task_success_total', 'Total number of successful tasks', labels=labels, registry=self.registry)
        self.job_task_failure = Counter('batch_job_task_failure_total', 'Total number of failed tasks', labels=labels, registry=self.registry)

        # Get Kafka and Dask addresses from environment variables
        kafka_servers = os.environ['KAFKA_SERVERS'].split(',')
        dask_address = os.environ['DASK_ADDRESS']

        # Create Kafka consumer
        self.consumer = KafkaConsumer('batch_job_trigger', bootstrap_servers=kafka_servers)

        # Create Dask client
        self.dask_client = Client(dask_address)

    def run(self):
        @task(name='Task D')
        def task_d():
            start_time = time.time()
            try:
                print('Running Task D')

                # Simulate processing 100 rows of data
                for i in range(100):
                    # Update progress
                    self.job_progress.labels(service='my_service').set(i + 1)

                    # Simulate processing time
                    time.sleep(0.1)

                # Simulate success
                print('Task D completed successfully')
                self.job_task_success.labels(service='my_service', task='Task D').inc()
                self.job_success.labels(service='my_service').inc()

            except Exception as e:
                print(f'Task D failed: {e}')
                self.job_task_failure.labels(service='my_service', task='Task D').inc()
                self.job_failure.labels(service='my_service').inc()

            end_time = time.time()
            duration = end_time - start_time
            self.job_duration.labels(service='my_service', task='Task D').observe(duration)

        # Define Prefect flow
        with Flow('Batch Job') as flow:
            task_d()

        # Run Prefect flow
        flow.run(executor=self.dask_client.get_executor())

    def listen(self):
        for message in self.consumer:
            # Run batch job when message is received
            self.run()

            # Get push gateway address from environment variable
            push_gateway_address = os.environ['PUSH_GATEWAY_ADDRESS']

            # Push metrics to push gateway
            push_to_gateway(push_gateway_address, job='batchA', registry=self.registry)

# Create batch job
job = BatchJob()

# Listen for messages from Kafka
job.listen()