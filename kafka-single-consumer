#!/usr/bin/python3
import threading
import matplotlib.pyplot as plt
from kafka import KafkaConsumer
from queue import PriorityQueue
import os
import json
import time
import ast
import traceback

priority_queue = PriorityQueue()
kafka_org_latency = {}
kakfa_global_order_latency = {}
flag = 0


def consume():
    try:
        global priority_queue, kafka_org_latency, flag
        consumer = KafkaConsumer('design',
                                 group_id='mygroup',
                                 auto_offset_reset='earliest', enable_auto_commit=True,
                                 bootstrap_servers=['localhost:9092'])

        for message in consumer:
            print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                 message.offset, message.key,
                                                 message.value))
            msg = (json.loads((message.value).decode("utf-8").replace("'", '"')))
            key = int((message.key).decode("utf-8"))
            priority_queue.put((key, msg))
            kafka_org_latency[key] = (time.time() * 1000) - msg['time_stamp']
            if priority_queue._qsize() > 20:
                deliver()
    except KeyboardInterrupt:
        print("Key board interrupt")
        if flag == 0:
            plotFunction()
            flag = 1
        traceback.print_exc


def deliver():
    global priority_queue, kakfa_global_order_latency
    print("size before pop", priority_queue._qsize())
    size = min(priority_queue._qsize(), 20)
    for i in range(size):
        key, val = priority_queue._get()
        print("POP VALUE {} {}".format(key, val))
        kakfa_global_order_latency[key] = (time.time() * 1000 - val['time_stamp'])
    print("size after pop", priority_queue._qsize())


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
                print("ok")
                x_axis.append(val)
                print("append to x")
                y1.append(kakfa_global_order_latency[val] - kafka_org_latency[val])
                y2.append(kafka_org_latency[val])
                y3.append(kakfa_global_order_latency[val])
        print("See this ")
        print("See the list " + str(y1))
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


consume()
