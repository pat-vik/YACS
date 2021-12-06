#Importing all the necessary modules required
import json
import socket
import sys
import threading
import time
from collections import deque
import random
from abc import ABC, abstractmethod

#Initializing the port numbers according to the given specifications
MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5000
MASTER_PORT_WORKER = 5001

#Initializing a deque data structure for storing Requests
REQUEST_Q = deque()
JOBS = {}

#Sending the task details to the worker
def send_request(task, worker_host, worker_port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((worker_host, worker_port))
        s.send(task.encode())   # send encrypted task

#Function which listens to the requests.py file in order to receive job and task details
#Threading is used in order to carry out this process
def master_request_listener():
    s = socket.socket()
    s.bind((MASTER_HOST, MASTER_PORT))
    s.listen(5)

    while True:
        print(REQUEST_Q, [job.__str__() for job in JOBS.values()])
        conn, addr = s.accept()
        request_data = json.loads(conn.recv(2048).decode('utf-8'))
        job_obj = Job(request_data)
        REQUEST_Q.append(job_obj.job_id)
        JOBS[job_obj.job_id] = job_obj

# Here we check if there is any job request present in the deque and then send it
# to the send_request function if that job is not yet finished. If it is
# is finished then we delete that job request from the REQUEST_Q(deque).
# Threading is used here.
def schedule_task(scheduler):
    while True:
        if REQUEST_Q:
            job_id = REQUEST_Q.popleft()
            job_obj = JOBS[job_id]
            if job_obj.is_job_finished():
                with open("master.logs", 'a') as f:
                    msg = "Job id: {} finished in {} second(s)\n".format(job_id, time.time() - job_obj.start_time)
                    print(msg)
                    f.write(msg)
                del JOBS[job_id]
            else:
                REQUEST_Q.append(job_id)
                id, task_id, duration = job_obj.get_next_avail_task()
                if id is not None:
                    worker_host, worker_port = scheduler.get_next_worker()
                    while worker_host is None:
                        time.sleep(1)
                        worker_host, worker_port = scheduler.get_next_worker()
                    task = {"job_id": id, "task_id": task_id, "duration": duration}
                    send_request(json.dumps(task), worker_host, worker_port)
        else:
            time.sleep(1)

# Using the threading concept we listen continuously to the worker for updates
# on the status of the tasks( i.e. which tasks are finished).
def master_worker_listener(scheduler):
    s1 = socket.socket()
    s1.bind((MASTER_HOST, MASTER_PORT_WORKER))
    s1.listen(5)
    print("Worker started listening to tasks")
    while True:
        c1, ad1 = s1.accept()
        finished_task = json.loads(c1.recv(2048).decode("utf-8"))
        print("***********", finished_task, REQUEST_Q, [job.__str__() for job in JOBS.values()])
        scheduler.mark_task_complete(finished_task["worker_id"])
        JOBS[finished_task["job_id"]].mark_task_complete(finished_task["task_id"])
        print(finished_task)

# This class is used to keep track of available slots, hostname, port of the worker.
class Worker:
    def __init__(self, id, hostname, port, slots):
        self.id = id
        self.hostname = hostname
        self.port = port
        self.available_slots = slots

# This class is used to keep track of the workers (for example - how many workers are present)
# and of the tasks that are completed.
# In short it acts as a helper to other functions.
class Scheduler(ABC):
    def __init__(self, config_path):
        worker_config = json.load(open(config_path, 'r'))["workers"]
        print(worker_config)
        self.workers = []
        for workers in worker_config:
            self.workers.append(Worker(str(workers["worker_id"]), "localhost", workers["port"], workers["slots"]))
        self.workers.sort(key=lambda x: x.id)

    @abstractmethod
    def get_next_worker(self):
        pass

    def mark_task_complete(self, worker_id):
        workers = [worker for worker in self.workers if worker.id == worker_id]
        print([worker.id for worker in self.workers], worker_id)
        print(type(self.workers[0].id), type(worker_id))
        print(workers)
        assert len(workers) == 1
        workers[0].available_slots += 1

    def _get_available_workers(self):
        return [worker for worker in self.workers if worker.available_slots > 0]

#Round Robin Scheduling
class RR(Scheduler):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.current_worker = 0

    def get_next_worker(self):
        if not self._get_available_workers():
            return None, None
        while self.workers[self.current_worker].available_slots == 0:
            self.current_worker = (self.current_worker + 1) % len(self.workers)
        worker = self.workers[self.current_worker]
        worker.available_slots -= 1
        self.current_worker = (self.current_worker + 1) % len(self.workers)
        return worker.hostname, worker.port

# Random Scheduling(using random.randInt)
class Random(Scheduler):
    def get_next_worker(self):
        available_workers = self._get_available_workers()
        if not available_workers:
            return None, None
        random_worker = available_workers[random.randint(0, len(available_workers) - 1)]
        random_worker.available_slots -= 1
        return random_worker.hostname, random_worker.port

# Least Loaded scheduling
class LL(Scheduler):
    def get_next_worker(self):
        available_workers = self._get_available_workers()
        if not available_workers:
            return None, None
        max_capacity = available_workers[0].available_slots
        max_capacity_worker = available_workers[0]
        for worker in available_workers:
            if worker.available_slots > max_capacity:
                max_capacity = worker.available_slots
                max_capacity_worker = worker
        max_capacity_worker.available_slots -= 1
        return max_capacity_worker.hostname, max_capacity_worker.port

# Class Job(Abstraction class) which is used to keep track of the progress of the tasks of each job.
# We implement functions such as mark_task_complete(Used when a task is completed),
# get_next_avail_task(Used to get the next available task of a job -
# for example when all the map tasks are completed then the reduce tasks will be executed),
# is_job_complete(Used when all the tasks of a job are completed).
class Job:
    def __init__(self, job):
        self.job_id = job["job_id"]
        self.map_tasks = {}
        self.reduce_tasks = {}
        self.scheduled_map_tasks = {}
        self.scheduled_reduce_tasks = {}
        self.start_time = time.time()
        m_tasks = job["map_tasks"]
        for task in m_tasks:
            self.map_tasks[task["task_id"]] = task["duration"]
        r_tasks = job["reduce_tasks"]
        for task in r_tasks:
            self.reduce_tasks[task["task_id"]] = task["duration"]

    def mark_task_complete(self, task_id):
        if task_id in self.scheduled_map_tasks:
            del self.scheduled_map_tasks[task_id]
        else:
            del self.scheduled_reduce_tasks[task_id]

    def get_next_avail_task(self):
        if self.map_tasks:
            print("$$$$$$$$$$$$$", self.map_tasks)
            task_id = next(iter(self.map_tasks))
            duration = self.map_tasks[task_id]
            self.scheduled_map_tasks[task_id] = duration
            del self.map_tasks[task_id]
            return self.job_id, task_id, duration
        elif self.scheduled_map_tasks:
            return None, None, None
        elif self.reduce_tasks:
            task_id = next(iter(self.reduce_tasks))
            duration = self.reduce_tasks[task_id]
            self.scheduled_reduce_tasks[task_id] = duration
            del self.reduce_tasks[task_id]
            return self.job_id, task_id, duration
        else:
            return None, None, None

    def is_job_finished(self):
        if not self.map_tasks and not self.reduce_tasks and not self.scheduled_map_tasks and not self.scheduled_reduce_tasks:
            return True
        return False

    def __str__(self):
        job = {"job_id": self.job_id, "map_tasks": self.map_tasks, "reduce_tasks": self.reduce_tasks,
               "scheduled_map_tasks": self.scheduled_map_tasks,
               "scheduled_reduce_tasks": self.scheduled_map_tasks}
        return str(job)

# In the main, after taking all the required inputs(arguments) we start 3 threads
# 1. - to keep listening to the requests
# 2. - scheduling the task to various workers according to given scheduling algorithm
# 3. - to keep listening to the workers about the updates on the tasks
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python master.py <config path> <algo>")
        exit()
    sl=open("Algorithm.logs","w")
    config_path = sys.argv[1]
    scheduling_algorithm = sys.argv[2]
    scheduler = None
    if scheduling_algorithm == "RR":
        scheduler = RR(config_path)
        sl.write("Round Robin Scheduling\n")
    elif scheduling_algorithm == "LL":
        scheduler = LL(config_path)
        sl.write("Least Loaded Scheduling\n")
    elif scheduling_algorithm == "RANDOM":
        scheduler = Random(config_path)
        sl.write("Random Scheduling\n")
    else:
        print("Invalid Scheduling Algorithm given. Supported algorithms are: RR, RANDOM, LL")
        exit()
    sl.close()
    open("master.logs", 'w').close()
    thread1 = threading.Thread(target=master_request_listener, args=())
    thread2 = threading.Thread(target=schedule_task, args=([scheduler]))
    thread3 = threading.Thread(target=master_worker_listener, args=([scheduler]))

    thread1.start()
    thread2.start()
    thread3.start()
