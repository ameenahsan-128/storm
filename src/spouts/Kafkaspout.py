from datetime import datetime, timedelta  # Importing the necessary modules
from collections import deque
from streamparse import Spout
from kafka import KafkaConsumer, errors
import time
import threading

class KafkaSpout(Spout):  # Defining a class named KafkaSpout that extends the Spout class

    outputs = ['message']  # Defining the output fields of the spout

    def initialize(self, stormconf, context):  # Initializing the spout
        self.message_queue = deque()  # Creating a deque to store the messages
        self.retry_interval = 15  # Setting the retry interval to 15 seconds
        self.is_running = True  # Flag to indicate if the spout is running or not

        # Start the Kafka consumer thread
        kafka_consumer_thread = threading.Thread(target=self.consume_kafka)  # Creating a thread for consuming Kafka messages
        kafka_consumer_thread.start()  # Starting the thread


    def connect_consumer(self):  # Method to create a Kafka consumer and connect to the Kafka server
        try:
            consumer = KafkaConsumer(
                'test',  # The topic to consume messages from
                bootstrap_servers='localhost:9092',  # The Kafka server address
                group_id='group1',  # The consumer group ID
                auto_offset_reset='earliest'  # Setting the offset to the earliest available
            )
            return consumer  # Returning the Kafka consumer object
        except errors.NoBrokersAvailable:
            self.log('No brokers available. Retrying in {} seconds...'.format(self.retry_interval))  # Logging an error message
            time.sleep(self.retry_interval)  # Sleeping for the retry interval
            return None  # Returning None if no brokers are available

    def consume_kafka(self):  # Method for consuming messages from Kafka
        consumer = self.connect_consumer()  # Creating a Kafka consumer

        while self.is_running:  # Loop to continuously consume messages
            if consumer is None:  # If consumer is None, try to reconnect
                consumer = self.connect_consumer()  # Reconnect to Kafka
                continue

            try:
                message = consumer.poll(timeout_ms=1000, max_records=1)  # Polling for new messages

                if message:  # If messages are received
                    for tp, messages in message.items():  # Iterating over the received messages
                        for msg in messages:  # Iterating over individual messages
                            self.message_queue.append(msg.value.decode('utf-8'))  # Adding the message to the queue

                    consumer.commit()  # Committing the offset to mark the messages as consumed

            except errors.KafkaError as e:  # Handling Kafka errors
                self.log('Kafka error: {}. Retrying in {} seconds...'.format(e, self.retry_interval))  # Logging the error message
                time.sleep(self.retry_interval)  # Sleeping for the retry interval
                consumer = self.connect_consumer()  # Reconnect to Kafka

        if consumer is not None:  # If the consumer is not None, close it
            consumer.close()  # Closing the Kafka consumer

    def next_tuple(self):  # Method to emit the next tuple
        if self.message_queue:  # If there are messages in the queue
            message = self.message_queue.popleft()  # Pop the first message from the queue
            self.emit([message])  # Emitting the message as a tuple
            self.log(message)

    def ack(self, tup_id):  # Method to handle successful processing of a tuple
        pass  # Doing nothing for now

    def fail(self, tup_id):  # Method to handle failed processing of a tuple
        self.log('Failed to process tuple: {}'.format(tup_id))  # Logging an error message

    # def stop(self):  # Method to stop the spout
    #     self.is_running = False  # Setting the flag to stop the spout
