#!/usr/bin/python3
from sqlite3 import Time
import threading
import time
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

exitFlag = 0
key_id = 0


class myThread(threading.Thread):
    arr = []

    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.arr = os.listdir("input")
        self.index = 0

    def run(self):
        print("Starting " + self.name)
        process_files_parallel(self.arr)
        print("Exiting " + self.name)


def producer_kafka(message):
    try:
        global key_id
        key = str(key_id).encode('utf-8')
        key_id = key_id + 1
        message = message.strip()
        send_ts = time.time() * 1000
        val = {"time_stamp": send_ts, "body": message}
        val = str(val).encode('utf-8')
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        producer.send('design1', key=key, value=val)
        producer.flush()
        producer = KafkaProducer(retries=5)
    except Exception as e:
        print("Exception in producer kafka " + str(e))


def process_file(name):
    """ Process one file: count number of lines and words """
    print("Processing file {}".format(name))
    with open(name, 'r') as inp:
        for line in inp:
            producer_kafka(line)
            # print(line)


def process_files_parallel(arr):
    global key_id
    try:
        dir = "input/"
        for index in range(0, len(arr)):
            file_name = arr[index]
            index = index + 1
            process_file(dir + file_name)
            # if key_id > 100:
            #     break
    except Exception as e:
        print("Caught in exception {}".format(str(e)))


# Create new threads
thread1 = myThread(1, "Thread-1")
thread2 = myThread(2, "Thread-2")

# Start new Threads
thread1.start()
thread2.start()
thread1.join()
thread2.join()
print("Exiting Main Thread {}".format(key_id))
