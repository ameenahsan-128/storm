from streamparse import Bolt
import subprocess
import json
import pickle
from collections import deque
import time
import threading
import os


# class SparkExecutionBolt(Bolt):
#     outputs = ['status']
#     spark_job_queue = deque()
#     failed_spark_job_queue = deque()

#     def initialize(self, storm_conf, context):
#         # Start the thread for executing Spark jobs
#         spark_thread = threading.Thread(target=self.execute_spark_job)
#         spark_thread.start()

#     def process(self, tup):
#         hdfs_file_path = tup.values[0]
#         document = json.loads(hdfs_file_path)
#         namenode_host = document.get('namenode_host')
#         email = document.get('email')
#         directory_path=document.get("directory")
        
#         self.log(hdfs_file_path)
#         self.log(email)
#         self.log(directory_path)
        
#     def execute_spark_job(self,namenode_host,email,directory_path,hdfs_file_path):

#         fullpath = f'hdfs://{namenode_host}:9000/{directory_path}/{str(email)}.txt'
        
#         # Add the Spark job command to the queue
#         command = f' --master local[*] /home/ameen/ner.py {fullpath} {email}'
#         self.spark_job_queue.append(command)

#         try:
#             with open('/home/ameen/storm/spark_job_queue.pickle', 'rb') as f:
#                 self.spark_job_queue = pickle.load(f)
#         except FileNotFoundError:
#             # Handle the case when the pickle file is not found
#             self.spark_job_queue = deque()

#             while True:
#                 if len(self.spark_job_queue) > 0:
#                     command = self.spark_job_queue.popleft()  # Use popleft() instead of pop(0) for deque object
#                     try:
#                         # Specify the Spark installation directory
#                         spark_submit_path = os.path.join('spark-submit')  # Path to spark-submit executable
#                         self.log(spark_submit_path + command)

#                         process = subprocess.Popen([spark_submit_path] + command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#                         stdout, stderr = process.communicate()
#                         process.wait()
#                         # Check the return code
#                         if process.returncode != 0:
#                             self.log(f'Error executing Spark job. Return code: {process.returncode}')
#                             self.failed_spark_job_queue.append(command)  # Add the failed Spark job command to the failed queue
#                         else:
#                             self.log('Spark job executed successfully.')
#                             with open('/home/ameen/storm/spark_job_queue.pickle', 'wb') as f:
#                                 pickle.dump(self.spark_job_queue, f)
#                             self.log('pickle file updated')
#                             self.emit((hdfs_file_path))

#                     except Exception as e:
#                         self.log(f'Error executing Spark job: {e}')
#                         self.failed_spark_job_queue.append(command)  # Add the failed Spark job command to the queue
#                         with open('/home/ameen/storm/spark_job_failed_queue.pickle', 'wb') as u:
#                                 pickle.dump(self.failed_spark_job_queue, u)
#                         self.log('pickle faile_job file updated')
                             
#                 else:
#                     # If the queue is empty, wait for a while and check again
#                     time.sleep(1)

#         try:
#             with open('/home/ameen/storm/spark_job_queue.pickle', 'rb') as f:
#                 self.failed_spark_job_queue = pickle.load(f)
#         except FileNotFoundError:
#             # Handle the case when the pickle file is not found
#             self.failed_spark_job_queue = deque()
#             while True:
#                 if len(self.failed_spark_job_queue) > 2:
#                     command = self.failed_spark_job_queue.popleft()  # Use popleft() instead of pop(0) for deque object
#                     try:
#                         # Specify the Spark installation directory
#                         spark_submit_path = os.path.join('spark-submit')  # Path to spark-submit executable
#                         self.log(spark_submit_path + command)

#                         process = subprocess.Popen([spark_submit_path] + command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#                         stdout, stderr = process.communicate()
#                         process.wait()
#                         # Check the return code
#                         if process.returncode != 0:
#                             self.log(f'Error executing Spark job. Return code: {process.returncode}')
#                             self.failed_spark_job_queue.append(command)  # Add the failed Spark job command to the failed queue
#                         else:
#                             self.log('Spark job executed successfully.')
#                             with open('/home/ameen/storm/spark_job_queue.pickle', 'wb') as f:
#                                 pickle.dump(self.failed_spark_job_queue, f)
#                             self.log('pickle file updated')
#                             self.emit((hdfs_file_path))

#                     except Exception as e:
#                         self.log(f'Error executing Spark job: {e}')
                             
#                 else:
#                     # If the queue is empty, wait for a while and check again
#                     time.sleep(1)


# from streamparse import Bolt
# import subprocess
# import json
# import pickle
# from collections import deque
# import time
# import threading
# import os


# class SparkExecutionBolt(Bolt):
#     outputs = ['status']
#     failed_spark_job_queue = deque()

#     def initialize(self, storm_conf, context):
#         # Start the thread for executing Spark jobs
#         spark_thread = threading.Thread(target=self.execute_spark_job_thread)
#         spark_thread.start()

#     def process(self, tup):
#         hdfs_file_path = tup.values[0]
#         document = json.loads(hdfs_file_path)
#         namenode_host = document.get('namenode_host')
#         email = document.get('email')
#         directory_path = document.get("directory")

#         self.log(hdfs_file_path)
#         self.log(email)
#         self.log(directory_path)

#         # Add the Spark job command to the queue
#         fullpath = f'hdfs://{namenode_host}:9000/{directory_path}/{str(email)}.txt'
#         command = f' --master local[*] /home/ameen/ner.py {fullpath} {email}'
#         self.spark_job_queue.append((command, namenode_host, email, directory_path, hdfs_file_path))

#     def execute_spark_job_thread(self):
#         while True:
#             if len(self.spark_job_queue) > 0:
#                 command, namenode_host, email, directory_path, hdfs_file_path = self.spark_job_queue.popleft()
#                 spark_thread = threading.Thread(target=self.execute_spark_job, args=(namenode_host, email, directory_path, hdfs_file_path, command))
#                 spark_thread.start()

#             else:
#                 # If the queue is empty, wait for a while and check again
#                 time.sleep(1)

#     def execute_spark_job(self, namenode_host, email, directory_path, hdfs_file_path, command):
#         try:
#             # Specify the Spark installation directory
#             spark_submit_path = os.path.join('spark-submit')  # Path to spark-submit executable
#             self.log(spark_submit_path + command)

#             process = subprocess.Popen([spark_submit_path] + command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#             stdout, stderr = process.communicate()
#             process.wait()
#             # Check the return code
#             if process.returncode != 0:
#                 self.log(f'Error executing Spark job. Return code: {process.returncode}')
#                 self.failed_spark_job_queue.append(command)  # Add the failed Spark job command to the failed queue
#             else:
#                 self.log('Spark job executed successfully.')
#                 self.emit([hdfs_file_path])

#         except Exception as e:
#             self.log(f'Error executing Spark job: {e}')
#             self.failed_spark_job_queue.append(command)  # Add the failed Spark job command to the queue
#             with open('/home/ameen/storm/spark_job_failed_queue.pickle', 'wb') as u:
#                 pickle.dump(self.failed_spark_job_queue, u)
#             self.log('pickle file updated')



class SparkExecutionBolt(Bolt):
    outputs = ['status']
    failed_spark_job_queue = deque()
    spark_job_queue = deque()

    executing_spark_job = False

    def initialize(self, storm_conf, context):
        # Start the thread for executing Spark jobs
        spark_thread = threading.Thread(target=self.execute_spark_job_thread)
        spark_thread.start()

    def process(self, tup):
        hdfs_file_path = tup.values[0]
        document = json.loads(hdfs_file_path)
        namenode_host = document.get('namenode_host')
        email = document.get('email')
        directory_path = document.get("directory")

        self.log(hdfs_file_path)
        self.log(email)
        self.log(directory_path)

        # Add the Spark job command to the queue
        fullpath = f'hdfs://{namenode_host}:9000/{directory_path}/{str(email)}.txt'
        command = f' --master local[*] /home/ameen/ner.py {fullpath} {email}'
        self.spark_job_queue.append((command, namenode_host, email, directory_path, hdfs_file_path))

    def execute_spark_job_thread(self):
        while True:
            if len(self.spark_job_queue) > 0 and not self.executing_spark_job:
                self.executing_spark_job = True
                command, namenode_host, email, directory_path, hdfs_file_path = self.spark_job_queue.popleft()
                self.execute_spark_job(namenode_host, email, directory_path, hdfs_file_path, command)
            else:
                # If the queue is empty or a job is already executing, wait for a while and check again
                time.sleep(1)

    def execute_spark_job(self, namenode_host, email, directory_path, hdfs_file_path, command):
        try:
            # Specify the Spark installation directory
            spark_submit_path = os.path.join('spark-submit')  # Path to spark-submit executable
            self.log(spark_submit_path + command)

            process = subprocess.Popen([spark_submit_path] + command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            process.wait()
            # Check the return code
            if process.returncode != 0:
                self.log(f'Error executing Spark job. Return code: {process.returncode}')
                self.failed_spark_job_queue.append(command)  # Add the failed Spark job command to the failed queue
                self.emit([hdfs_file_path])
                
            else:
                self.log('Spark job executed successfully.')
                self.emit([hdfs_file_path])

        except Exception as e:
            self.log(f'Error executing Spark job: {e}')
            self.spark_job_queue.append(command)  # Add the failed Spark job command to the queue

        self.executing_spark_job = False


# import os
# import subprocess
# import pickle
# from collections import deque
# import threading
# import time
# import json
# from streamparse import Bolt


# class SparkExecutionBolt(Bolt):
#     outputs = ['status']

#     def initialize(self, storm_conf, context):
#         self.failed_spark_job_queue = deque()
#         self.spark_job_queue = deque()
#         self.executing_spark_job = False
#         # Start the thread for executing Spark jobs
#         spark_thread = threading.Thread(target=self.execute_spark_job_thread)
#         spark_thread.start()

#     def process(self, tup):
#         self.hdfs_file_path = tup.values[0]
#         self.document = json.loads(self.hdfs_file_path)
#         self.namenode_host = self.document.get('namenode_host')
#         self.email = self.document.get('email')
#         self.directory_path = self.document.get("directory")

#         # Add the Spark job command to the queue
#         fullpath = f'hdfs://{self.namenode_host}:9000/{self.directory_path}/{str(self.email)}.txt'
#         command = f'--master local[*] /home/ameen/ner.py {fullpath} {self.email}'
#         self.spark_job_queue.append((self.hdfs_file_path, command))

#     def execute_spark_job_thread(self):
#         while True:
#             if len(self.spark_job_queue) > 0 and not self.executing_spark_job:
#                 self.executing_spark_job = True
#                 hdfs_file_path, command = self.spark_job_queue.popleft()
#                 self.execute_spark_job(hdfs_file_path, command)

#             else:
#                 # If the queue is empty or a job is already executing, wait for a while and check again
#                 time.sleep(1)

#     def execute_spark_job(self, hdfs_file_path, command):
#         try:
#             # Specify the Spark installation directory
#             spark_submit_path = os.path.join('spark-submit')  # Path to spark-submit executable

#             process = subprocess.Popen([spark_submit_path] + command.split(), stdout=subprocess.PIPE,
#                                        stderr=subprocess.PIPE)
#             stdout, stderr = process.communicate()
#             process.wait()
#             # Check the return code
#             if process.returncode != 0:
#                 self.log(f'Error executing Spark job. Return code: {process.returncode}')
#                 self.failed_spark_job_queue.append(command)  # Add the failed Spark job command to the failed queue
#                 pickle_file = '/home/ameen/storm/spark_job_failed_queue.pickle'
#                 if not os.path.exists(pickle_file):
#                     with open(pickle_file, 'wb') as f:
#                         pickle.dump(list(self.failed_spark_job_queue), f)
#             else:
#                 self.log('Spark job executed successfully.')
#                 self.emit([hdfs_file_path])

#         except Exception as e:
#             self.log(f'Error executing Spark job: {e}')
#             self.spark_job_queue.append((hdfs_file_path, command))  # Add the failed Spark job command to the queue again
