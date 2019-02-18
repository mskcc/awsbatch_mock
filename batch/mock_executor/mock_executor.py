import time
import uuid
from datetime import datetime, timedelta
import threading


JOB_RUNNING_TIME = {
    "root.bwa_mem": timedelta(seconds=1),
    "root.make_bam": timedelta(seconds=1),
    "root.sort_bam": timedelta(seconds=1),
    "root.mark_duplicates": timedelta(seconds=1),
    "root.base_recal": timedelta(seconds=1),
    "root.apply_bqsr": timedelta(seconds=1)
}

completed = []

jobs = {}


class MockJob(object):

    def __init__(self, job_name, name):
        self.job_id = uuid.uuid4()
        self.job_name = job_name
        self.name = name
        self.status = 'RUNNING'
        self.started = datetime.now()
        self.completed = None

    def dump(self):
        return {
            "jobId": self.job_id,
            "jobName": self.job_name,
            "createdAt": str(int(self.started.timestamp()*1000.0)),
            "status": self.status,
            "statusReason": "Essential container in task exited",
            "startedAt": str(int(self.started.timestamp()*1000.0)),
            "stoppedAt": str(int(self.completed.timestamp()*1000.0)),
            "container": {
                "exitCode": 0
            }
        }

    def __str__(self):
        return "ID: %s NAME: %s, STATUS: %s" %(self.job_id, self.name, self.status)


class JobService(object):

    def __init__(self, workers):
        self.workers = workers
        self.curr = 0
        global jobs
        for i in range(workers):
            jobs[i] = []
        print("Created JobService")

    def start_job(self, job_id, name):
        jobs[self.curr].append(MockJob(job_id, name))
        self.curr = (self.curr + 1) % self.workers


class Worker(threading.Thread):

    def __init__(self, worker_id, tasks_db):
        self.worker_id = worker_id
        # self.js = job_service
        self.tasks_db = tasks_db
        print("started worker %d" % worker_id)
        super(Worker, self).__init__()

    def run(self):
        global completed
        while(True):
            time.sleep(1)
            for task in self.tasks_db:
                if task.status == "RUNNING":
                    if datetime.now() - task.started > JOB_RUNNING_TIME[task.name]:
                        print("Job SUCCEEDED: %s %s" % (task.job_name, task.name))
                        task.completed = datetime.now()
                        task.status = "SUCCEEDED"
                        completed.append(task)


def start_mock_executor(threads):
    js = JobService(threads)
    for i in range(threads):
        p = Worker(i, jobs[i])
        p.start()
    return js
