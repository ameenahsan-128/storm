# from streamparse import Bolt
# import json
# from datetime import datetime
# from pymongo import MongoClient
# import requests

# class HDFS(Bolt):
#     outputs = []

#     def initialize(self, storm_conf, context):
#         # Connect to MongoDB
#         pass

#     def process(self, tup):
#         hdfs_file_path = tup.values[0]
#         self.log(hdfs_file_path)
#         document = json.loads(hdfs_file_path)
#         hdfs_file_path = document.get('path')
#         email = document.get('email')
#         directory_path=document.get("directory")
#         self.log(email)
#         self.log(directory_path)

#         client = MongoClient('mongodb://ec2-35-154-223-156.ap-south-1.compute.amazonaws.com:27017/')
#         mongo_db = 'username'
#         db = client[mongo_db]

#         # Construct the WebHDFS URL to check the status of the file
#         namenode_host = 'localhost'
#         namenode_port = '9870'
#         api_url = f'http://{namenode_host}:{namenode_port}/webhdfs/v1/{directory_path}/{email}.txt/?op=GETFILESTATUS'
#         self.log(api_url)

#         try:
#             response = requests.get(api_url)
#             if response.status_code == 200:
#                 # File status retrieved successfully
#                 file_status = response.json()
#                 self.log("HDFS file status: " + json.dumps(file_status))
#                 # Perform further processing or actions based on the file status
#                 # user = db.test.find_one({"email": email})

#                 # if "email" in user:
#                 db.test.insert_one({"$set":{"Successfully converted to text": True}})

#             else:
#                 self.log(f"Failed to check HDFS file status. Status code: {response.status_code}")
#                 # Update the file status in MongoDB
#                 db.test.insert_one({"$set":{"Successfully converted to text": False}})
        

#         except requests.exceptions.RequestException as e:
#             self.log("Error checking HDFS file status:", str(e))
#             # Update the file status in MongoDB

# import time
# from streamparse import Bolt
# import json
# from pymongo import MongoClient
# import requests
# from pymongo.errors import PyMongoError
# from collections import deque


# class HDFS(Bolt):
#     outputs = ['output']

#     def initialize(self, storm_conf, context):
#         self.client = MongoClient('mongodb://ec2-35-154-223-156.ap-south-1.compute.amazonaws.com:27017/')
#         self.mongo_db = 'username'
#         self.db = self.client[self.mongo_db]
#         self.api_url_queue = deque()
#         self.namenode_host = 'localhost'
#         self.namenode_port = '9870'

#     def process(self, tup):
#         self.hdfs_file_path = tup.values[0]
#         self.document = json.loads(self.hdfs_file_path)
#         self.email = self.document.get('email')
#         self.directory_path = self.document.get("directory")

#         try:
#             if self.hdfs_file_path is not None:
#                 self.api_url = f'http://{self.namenode_host}:{self.namenode_port}/webhdfs/v1/{self.directory_path}/{self.email}.txt/?op=GETFILESTATUS'
#                 # self.log(api_url)
#                 self.api_url_queue.append(self.api_url)
#                 self.process_documents()
#         except Exception as e:
#             self.log(f'Error checking status: {e}')
            
#     def process_documents(self):
#         try:
#             server_info = self.client.server_info()
#             # Check the status code to determine server availability
#             if server_info["ok"] == 1:
#                 while self.api_url_queue:
#                     self.api_url = self.api_url_queue.popleft()
#                     self.HDFS_STATUS()
                
#         except PyMongoError:
#         # An error occurred while connecting or pinging the server
#              self.log("connection Error:")

#     def HDFS_STATUS(self):
#             try:
#                 response = requests.get(self.api_url)
#                 if response.status_code == 200:
#                     result=self.db.test.insert_one({"$set": {"Successfully converted to text": True}})
#                     self.log(result.inserted_id)

#                 else:
#                     self.log(f"Failed to check HDFS file status. Status code: {response.status_code}")
#                     result1=self.db.test.insert_one({"$set": {"Successfully converted to text": False}})
#                     self.log("fail"+result1.inserted_id)
#             except PyMongoError as e:
#                 self.log("Error occurred in PyMongo:", str(e))
#                 self.api_url_queue.append(self.api_url)
#             time.sleep(1)  # Delay between retries to avoid overwhelming the system



import threading
import time
from streamparse import Bolt
from pymongo import MongoClient
from collections import deque
import json
import requests
from pymongo.errors import PyMongoError

class HDFS(Bolt):
    outputs = ['output']

    def initialize(self, storm_conf, context):
        self.client = MongoClient('mongodb://ec2-35-154-223-156.ap-south-1.compute.amazonaws.com:27017/')
        self.mongo_db = 'username'
        self.db = self.client[self.mongo_db]
        self.api_url_queue = deque()
        self.namenode_host = 'localhost'
        self.namenode_port = '9870'

    def process(self, tup):
        self.hdfs_file_path = tup.values[0]
        self.document = json.loads(self.hdfs_file_path)
        self.email = self.document.get('email')
        self.directory_path = self.document.get("directory")

        try:
            if self.hdfs_file_path is not None:
                self.api_url = f'http://{self.namenode_host}:{self.namenode_port}/webhdfs/v1/{self.directory_path}/{self.email}.txt/?op=GETFILESTATUS'
                self.api_url_queue.append(self.api_url)
        except Exception:
            pass

        # Start a new thread to execute the process_documents function
        thread = threading.Thread(target=self.process_documents)
        thread.start()

    def process_documents(self):
        try:
            server_info = self.client.server_info()
            # Check the status code to determine server availability
            if server_info["ok"] == 1:
                while self.api_url_queue:
                    self.api_url = self.api_url_queue.popleft()
                    try:
                        response = requests.get(self.api_url)
                        if response.status_code == 200:
                            result = self.db.test.insert_one({"$set": {"Successfully converted to text": True}})
                            self.log(result.inserted_id)
                        else:
                            self.log(f"Failed to check HDFS file status. Status code: {response.status_code}")
                            result1 = self.db.test.insert_one({"$set": {"Successfully converted to text": False}})
                            self.log("fail" + result1.inserted_id)
                    except PyMongoError as e:
                        self.log("Error occurred in PyMongo:", str(e))
                        self.api_url_queue.append(self.api_url)
                    time.sleep(1)  # Delay between retries to avoid overwhelming the system
        except PyMongoError:
            # An error occurred while connecting or pinging the server
            self.log("Connection Error:")

   
            







        

        
