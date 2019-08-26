#!/bin/python3
import sys
import xml.etree.ElementTree as ET
import urllib.request as request
import json
import csv
import time
import os
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from queue import Queue

#JH_SERVER = "10.30.16.10:19888"
JH_SERVER = "0.0.0.0:19888"
JOBS_URL = "http://" + JH_SERVER + "/ws/v1/history/mapreduce/jobs"
OUTPUT_DIR = "./history"
C_OUTPUT_DIR = OUTPUT_DIR + "/counters"
TASKS_CSV_JID_IND = 8
TASKS_CSV_TID_IND = 4

metrics_header = ['submitTime', 'startTime', 'finishTime', 'id', 'name',
                  'queue', 'user', 'state', 'mapsTotal', 'mapsCompleted',
                  'reducesTotal', 'reducesCompleted', 'uberized',
                  'diagnostics', 'avgMapTime', 'avgReduceTime',
                  'avgShuffleTime', 'avgMergeTime', 'failedReduceAttempts',
                  'killedReduceAttempts', 'successfulReduceAttempts',
                  'failedMapAttempts', 'killedMapAttempts',
                  'successfulMapAttempts']
jobs_header = ["submitTime", "startTime", "finishTime", "id", "name", "queue",
               "user", "state", "mapsTotal", "mapsComplete", "reducesTotal",
               "reducesCompleted"]

attempts_header = ['startTime', 'finishTime', 'elapsedTime', 'progress',
                   'id', 'rack', 'state', 'status', 'nodeHttpAddress',
                   'diagnostics', 'type', 'assignedContainerId', 'tId',
                   'jId']

tasks_dict = {}

tasks_q = Queue()
task_counters_q = Queue()
metrics_queue = Queue()
job_counters_q = Queue()
attempts_q = Queue()

metrics_header = ['submitTime', 'startTime', 'finishTime', 'id', 'name', 'queue', 'user',
                'state', 'mapsTotal', 'mapsCompleted', 'reducesTotal', 'reducesCompleted',
                'uberized', 'diagnostics', 'avgMapTime', 'avgReduceTime', 'avgShuffleTime',
                'avgMergeTime', 'failedReduceAttempts', 'killedReduceAttempts',
                'successfulReduceAttempts', 'failedMapAttempts', 'killedMapAttempts',
                'successfulMapAttempts']
tasks_header = ["startTime", "finishTime", "elapsedTime", "progress", "id",
                 "state", "type", "successfulAttempt", "jobId"]
jc_header = ["jId", "counterGroupName", "name", "reduceCounterValue",
             "mapCounterValue", "totalCounterValue" ]
tc_header = ["tId", "counterGroupName", "name", "value" ]

def get_url(URL, error_msg=''):
    if not error_msg: error_msg = "Failed request url [{}]".format(URL)
    ret = None
    try:
        ret = request.urlopen(URL)
    except Exception as e:
        e = sys.exc_info()[0]
        print("ERROR:: {} MSG:{}".format(error_msg, str(e)))
    return ret;

def get_metrics(jobId):
    if not jobId:
        print("Null job id")
        return None
    url = JOBS_URL + "/" + jobId
    print("Getting url [{}]".format(url))
    metrics_obj = None
    metrics_obj = get_url(url)
    if metrics_obj:
        metrics_json = json.loads(metrics_obj.read())
        metrics_queue.put(metrics_json['job'])
        metrics_header = metrics_json['job'].keys()
        #print(metrics_json['job'].keys())


def get_job_counters(jobId):
    if not jobId:
        return None
    url = JOBS_URL + "/" + jobId + "/counters"
    print("Getting url [{}]".format(url))
    counters_obj = get_url(url)
    if not counters_obj:
        print("Error reading counters for job id = [{}]".format(jobId))
        return
    counters_json = json.loads(counters_obj.read())
    job_counters_q.put(counters_json['jobCounters'])

def check_dir_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)

def get_tasks(jobId):
    if not jobId:
        print("jobId === None")
    #print(tasks[0].keys())
        return None
    url = JOBS_URL + "/" + jobId + "/tasks"
    print("Getting url [{}]".format(url))
    tasks_obj = get_url(url)
    if not tasks_obj:
        print("ERROR: Failed to get tasks for job[{}]".format(jobId))
        return False
    tasks_json = json.loads(tasks_obj.read())
    tasks = tasks_json['tasks']['task']
    tasks_tuple = (jobId, tasks)
    tasks_q.put(tasks_tuple)
    tasks_dict[jobId] = tasks
    #print(tasks[0].keys())

def get_attempts(jId, tId):
    url = "{}/{}/tasks/{}/attempts".format(JOBS_URL, jId, tId)
    print("Getting attempt url [{}]".format(url))
    obj = get_url(url)
    if not obj:
        print("Error retrieving attemtps jId = [{}], tId = [{}]".format(jId, tId))
        return
    js = json.loads(obj.read())
    attempts = js['taskAttempts']['taskAttempt']
    values = []
    for attempt in attempts:
        attempt_v = list(attempt.values())
        attempt_v.append(tId)
        attempt_v.append(jId)
        attempts_q.put(attempt_v)


def get_task_attempts(tasks):
    if not tasks:
        print("Error tasks must not be null")
        return
    for task in tasks:
        tId = task[TASKS_CSV_TID_IND]
        jId = task[TASKS_CSV_JID_IND]
        url = "{}/{}/tasks/{}/attempts".format(JOBS_URL, jId, tId)
        print("Getting attempt url [{}]".format(url))
        obj = get_url(url)
        if not obj:
            print("Error retrieving attemtps jId = [{}], tId = [{}]".format(jId, tId))
            continue
        js = json.loads(obj.read())
        attempts = js['taskAttempts']['taskAttempt']
        values = []
        for attempt in attempts:
            attempt_v = list(attempt.values())
            attempt_v.append(tId)
            attempt_v.append(jId)
            values.append(attempt_v)
        attempts_q.put(values)

def get_task_counters(jobId, taskId):
    if not jobId or not taskId:
        print(">>ERROR: get_task_counters: jobId or taskId was null")
        return None
    url = "{}/{}/tasks/{}/counters".format(JOBS_URL, jobId, taskId)
    print("Getting url [{}]: params {} {}".format(url, jobId, taskId))
    counters_obj = get_url(url)
    counters_json = json.loads(counters_obj.read())
    tasks = counters_json['jobTaskCounters']['taskCounterGroup']
    task_counter_tuple = (jobId, taskId, tasks)
    task_counters_q.put(task_counter_tuple)

def write_jobs_csv(jobs, with_header=False):
    with open(OUTPUT_DIR + '/jobs.csv', 'w', newline='') as csvfile:
        jobswriter = csv.writer(csvfile, delimiter=',')
        if with_header:
            jobswriter.writerow(metrics_header)
        for job in jobs:
            jobswriter.writerow(job.values())

def write_attempts(attempts, path, with_header=False):
    write_csv(attempts, path, attempts_header)

def write_csv(rows, path, with_header=None):
    with open(path, "w+") as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        if with_header:
            writer.writerow(with_header)
        for row in rows:
            if type(row) == dict:
                writer.writerow(row.values())
            else:
                writer.writerow(row)

def call_get_job_counters(job_json):
    get_job_counters(job_json['id'])

def get_task_ids(task_list):
    id_list = []
    for task in task_list:
        id_list.append(task['id'])
    return id_list

def get_task_rows(jId, tasks):
    t_list = []
    for task in tasks:
        t_vals = list(task.values())
        t_vals.append(jId)
        t_list.append(t_vals)
    return t_list

def get_task_header(tasks):
    header = tasks[0].keys()
    header.append("jId")
    return header

def write_tasks(tasks_list, tasks):
    t_list = []
    for task_tuple in tasks_list:
        jId = task_tuple[0]
        tasks = task_tuple[1]
        rows = get_task_rows(jId, tasks)
        t_list = t_list + rows
    path = "{}/tasks.csv".format(OUTPUT_DIR)
    write_csv(t_list, path, with_header=tasks_header)

def write_job_counters(job_counter_list, path):
    job_rows = get_job_counter_rows(job_counter_list)
    write_csv(job_rows, path, with_header=jc_header)

def write_task_counters(counters, path):
    rows = get_task_counter_rows(counters)
    write_csv(rows, path, with_header=tc_header)

#def write_attempt(attempts, path):
 #   attempts_rows =

'''
return list of job rows
'''
def get_job_counter_rows(job_counter_list):
    #j_rows = {}
    j_rows = []
    for jc in job_counter_list:
        jId = jc['id']
        #if not jId in j_rows:
        #    j_rows[jId] = []
        groups = jc['counterGroup']
        for group in groups:
            c_group_name = group['counterGroupName']
            counters = group['counter']
            for counter in counters:
                c_name = counter['name']
                m_counter = counter['mapCounterValue']
                r_counter = counter['reduceCounterValue']
                t_counter = counter['totalCounterValue']
                row = [ jId, c_group_name, c_name, r_counter, m_counter, t_counter ]
                #j_rows[jId].append(row)
                j_rows.append(row)
    return j_rows

'''
return list of task rows
'''
def get_task_counter_rows(counter_list):
    #j_rows = {}
    rows = []
    for t_tuple in counter_list:
        tId = t_tuple[1]
        groups = t_tuple[2]
        for group in groups:
            c_group_name = group['counterGroupName']
            counters = group['counter']
            for counter in counters:
                c_name = counter['name']
                c_value = counter['value']
                row = [ tId, c_group_name, c_name, c_value ]
                rows.append(row)
    return rows



check_dir_exists(OUTPUT_DIR)
#check_dir_exists(C_OUTPUT_DIR)

print("-----------------Getting Job ID's-------------------")
jobs_obj = get_url(JOBS_URL)
print(jobs_obj)
jobs_json = jobs_obj.read()
jobs_json = json.loads(jobs_json)
jobs_list = jobs_json['jobs']['job']
job_ids = []
for job in jobs_list:
    job_ids.append(job['id'])
get_counters = True
##100 threads!!
with ThreadPoolExecutor(max_workers=150) as executor:
    futures = []
    print("--------------GETTING JOBS, TASKS and JOB-COUNTERS---------------------")
    for id in job_ids:
        futures.append(executor.submit(get_metrics, id))
        futures.append(executor.submit(get_tasks, id))
        futures.append(executor.submit(get_job_counters, id))
    ##DONT FORGET TO WAIT FOR tasks_q to be populated!!!
    wait(futures)
    futures = []
    tasks = list(tasks_q.queue)
    print("---------------GETTING ATTEMPTS----------------------")
    for task_t in tasks:
        jId = task_t[0]
        taskRows = get_task_rows(jId, task_t[1])
        for row in taskRows:
            jId = row[TASKS_CSV_JID_IND]
            tId = row[TASKS_CSV_TID_IND]
            futures.append(executor.submit(get_attempts, jId, tId))
        wait(futures)
    futures = []
    if get_counters:
        print("--------------GETTING TASK-COUNTERS---------------------")
        tasks = list(tasks_q.queue)
        for jId, tasks in tasks:
            tId_list = get_task_ids(tasks)
            for tId in tId_list:
                f = executor.submit(get_task_counters, jId, tId)
                futures.append(f)
        wait(futures)

    print("----------------WRITING FILES---------------")
    metrics = list(metrics_queue.queue)
    path = "{}/jobs.csv".format(OUTPUT_DIR)
    write_csv(metrics, path, with_header=metrics_header)

    tasks = list(tasks_q.queue)
    path = "{}/tasks.csv".format(OUTPUT_DIR)
    write_tasks(tasks, path)

    job_counters = list(job_counters_q.queue)
    path = "{}/job_counters.csv".format(OUTPUT_DIR)
    write_job_counters(job_counters, path)

    task_counters = list(task_counters_q.queue)
    path = "{}/task_counters.csv".format(OUTPUT_DIR)
    write_task_counters(task_counters, path)

    task_attempts = list(attempts_q.queue)
    path = "{}/attempts.csv".format(OUTPUT_DIR)
    write_attempts(task_attempts, path, with_header=True)


    print("------------")
    print("JOB_HEADER={}".format(metrics_header))
    print("TASK_HEADER={}".format(tasks_header))
    print("TC_HEADER={}".format(tc_header))
    print("JC_HEADER={}".format(jc_header))
    print("ATTEMPTS_HEADER={}".format(attempts_header))

    #write job counters
    #write task counters
    #write job counters
    #write task counters

