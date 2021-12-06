#Importing all the necessary modules required
import json
import socket
import sys
import threading
import time
from queue import PriorityQueue

#Initializing the port numbers according to the given specifications
task_queue = PriorityQueue()
master_host = "127.0.0.1"
master_port = 5001
worker_id = None

# This function is used to continuously listen to the master for tasks.
# After receiving the tasks it puts the tasks in a priority queue(execution pool).
# Threading is used here.
def task_reciver(port):
    print(port)
    s1 = socket.socket()
    s1.bind(("localhost", port))
    s1.listen(5)
    print("Worker started listening to tasks")
    while True:
        c1, ad1 = s1.accept()
        request = json.loads(c1.recv(2048).decode("utf-8"))
        job_id = request["job_id"]
        task_id = request["task_id"]
        duration = int(request["duration"])
        received_time = time.time()
        task_queue.put((received_time + duration, task_id, job_id, received_time))
        with open("worker_st.logs", 'a') as f:
            msg = "started task id: {} for job id {} at time {} by worker id {}\n".format(task_id, job_id,
                                                                                                time.time(),
                                                                                                worker_id)
            print(msg)
            f.write(msg)

# This function is used to check if a task is completed. If the task is completed then
# it calls the notify_master function.
# Threading is used in this process.
def notifier():
    print("Worker notifier started")
    wait_for_tasks = False
    while True:
        if wait_for_tasks:
            time.sleep(1)
        wait_for_tasks = True
        if not task_queue.empty():
            task = task_queue.get()
            if task[0] < time.time():
                notify_master(task[1], task[2], task[3])
                # it is possible that multiple tasks can be finished at same time, look for next task immediately.
                wait_for_tasks = False
            else:
                task_queue.put(task)

# This function is called by the notifier function in order to communicate to the master
# when a task gets completed. It also logs the time at which the task is completed.
def notify_master(task_id, job_id, received_time):
    finished_task = dict()
    finished_task["worker_id"] = worker_id
    finished_task["task_id"] = task_id
    finished_task["job_id"] = job_id
    with open("worker.logs", 'a') as f:
        msg = "finished task id: {} for job id {} in {} second(s) by worker id {}\n".format(task_id, job_id, time.time() - received_time, worker_id)
        print(msg)
        f.write(msg)
    with open("worker_ft.logs", 'a') as f:
        msg = "finished task id: {} for job id {} at time {} by worker id {}\n".format(task_id, job_id, time.time(), worker_id)
        print(msg)
        f.write(msg)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((master_host, master_port))
        message = json.dumps(finished_task)
        s.send(message.encode())

# In the main, after taking all the required inputs(arguments) we start 2 threads
# 1. - to keep listening to the tasks from the master
# 2. - notify the master when a task is done.
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python worker.py <port_number> <worker_id>")
        exit()

    port = int(sys.argv[1])
    worker_id = sys.argv[2]
    open("worker.logs", 'w').close()
    open("worker_st.logs", 'w').close()
    open("worker_ft.logs", 'w').close()
    task_receiver_thread = threading.Thread(target=task_reciver, args=([port]))
    notifier_thread = threading.Thread(target=notifier, args=())
    task_receiver_thread.start()
    notifier_thread.start()
    print("Starting worker at port:{}, worker_id:{}".format(port, worker_id))
