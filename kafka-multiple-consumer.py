#!/usr/bin/python3
import threading
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from queue import PriorityQueue
import os, json
import time
import traceback

priority_queue = PriorityQueue()
kafka_org_latency = {}
kakfa_global_order_latency = {}
flag = 0


class myThread(threading.Thread):
    arr = []

    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):
        print("Starting " + self.name)
        consume(self.threadID)
        print("Exiting " + self.name)


def consume(threadId):
    try:
        global priority_queue, kafka_org_latency, flag
        consumer = KafkaConsumer('design1',
                                 group_id='mygroup',
                                 auto_offset_reset='earliest', enable_auto_commit=True,
                                 bootstrap_servers=['localhost:9092'])

        for message in consumer:
            print("Thread ID %s %s:%d:%d: key=%s value=%s" % (threadId, message.topic, message.partition,
                                                              message.offset, message.key,
                                                              message.value))
            msg = (json.loads((message.value).decode("utf-8").replace("'", '"')))
            key = int((message.key).decode("utf-8"))
            priority_queue.put((key, msg))
            kafka_org_latency[key] = (time.time() * 1000) - msg['time_stamp']
            if priority_queue._qsize() > 40:
                deliver()
    except KeyboardInterrupt:
        print("Key board interupt")
        if flag == 0:
            plotFunction()
            flag = 1
        traceback.print_exc


def deliver():
    try:
        global priority_queue, kakfa_global_order_latency
        print("size before pop", priority_queue._qsize())
        size = min(priority_queue._qsize(), 20)
        for i in range(size):
            key, val = priority_queue._get()
            print("POP VALUE {} {}".format(key, val))
            kakfa_global_order_latency[key] = (time.time() * 1000 - val['time_stamp'])
        print("size after pop", priority_queue._qsize())
    except Exception as e:
        print(str(e))


def plotFunction():
    try:
        global kakfa_global_order_latency, kafka_org_latency
        x = kakfa_global_order_latency.keys()
        last_index = max(x)
        x_axis = []
        y1 = []
        y2 = []
        y3 = []
        for val in range(last_index):
            if val in kafka_org_latency and val in kakfa_global_order_latency:
                x_axis.append(val)
                y1.append(kakfa_global_order_latency[val] - kafka_org_latency[val])
                y2.append(kafka_org_latency[val])
                y3.append(kakfa_global_order_latency[val])
        kafka_avg = sum(y2) / len(y2)
        kafka_global = sum(y3) / len(y3)
        print("Average latecy of kafka {} average latency of modified kafka {}".format(kafka_avg, kafka_global))

        fig, axis = plt.subplots(3)

        axis[0].plot(x_axis, y1)
        axis[0].set_title('Latency Variation')
        axis[1].plot(x_axis, y2)
        axis[1].set_title('Kafka Latency ')
        axis[2].plot(x_axis, y3)
        axis[2].set_title('kafka modified latency')
        plt.show()
    except Exception as e:
        traceback.print_exc
        print(e)


# Create new threads
thread1 = myThread(1, "Thread-1")
thread2 = myThread(2, "Thread-2")
thread3 = myThread(3, "Thread-3")

# Start new Threads
try:
    thread1.start()
    thread2.start()
    # thread3.start()
    thread1.join()
    thread2.join()
    # thread3.join()
    plotFunction()
    print("Exiting Main Thread")
except KeyboardInterrupt:
    plotFunction()
