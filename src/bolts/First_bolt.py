# from streamparse import Bolt
# import requests
# from datetime import datetime
# import json
# from tika import parser

# class Emit(Bolt):
#     outputs = ['output']

#     def initialize(self, storm_conf, context):
#         pass

#     def process(self, tup):
#         document = tup.values[0]
#         document = json.loads(document)
#         hdfs_file_path = document.get('path')
#         email = document.get('email')

#         try:
#             if hdfs_file_path is not None:
#                 self.write_data_to_hdfs(hdfs_file_path, email)
#         except Exception as e:
#             self.log(f'Error processing document: {e}')

#     def write_data_to_hdfs(self, hdfs_file_path, email):
#         # WebHDFS endpoint and authentication
#         namenode_host = 'localhost'
#         namenode_port = '9870'
#         datanode_port = '9864'
#         username = 'ameen'
#         webhdfs_base_url = f'http://{namenode_host}:{namenode_port}/webhdfs/v1'
#         datanode_url = f'http://{namenode_host}:{datanode_port}/webhdfs/v1'
#         api_url = f'{webhdfs_base_url}{hdfs_file_path}/?op=OPEN'
#         self.log(api_url)

#         session = requests.Session()
#         response = session.get(api_url)

#         if response.status_code == 200:
#             self.log('Successfully read file from HDFS')
#             try:
#                 tika_server_url = 'http://localhost:9998'  # Replace with the appropriate Tika server URL
#                 parsed_data = parser.from_file(api_url, serverEndpoint=tika_server_url)
#                 text_content = parsed_data['content'].encode('utf-8').strip()
#                 self.log("text extraction done")
#                 # more text conversion logics neededsss

#                 # Get the current date
#                 current_date = datetime.now()
#                 year = current_date.strftime('%Y')
#                 month = current_date.strftime('%Y-%m')
#                 day = current_date.strftime('%Y-%m-%d')

#                 # Create the directory path based on the date hierarchy
#                 directory_path = f'{year}/{month}/{day}'

#                 # Create the WebHDFS URL to create the file
#                 create_url = f'{datanode_url}/{directory_path}/{str(email)}.txt?op=CREATE&user.name={username}&namenoderpcaddress={namenode_host}:9000&createflag=&createparent=true&overwrite=true'
#                 self.log(create_url)

#                 # Write the data to HDFS using WebHDFS
#                 headers = {'Content-Type': 'application/octet-stream'}
#                 response = session.put(create_url, data=text_content, headers=headers)

#                 if response.status_code == 201:
#                     self.log(f'Successfully wrote data to HDFS')

#                     tupvalues = json.dumps({"path": hdfs_file_path,"email": email , "directory":directory_path ,"namenode_host":namenode_host})
                    
#                     self.emit([tupvalues])

#                 else:
#                     self.log(f'Failed to write file to WebHDFS. Status code: {response.status_code}')


#             except Exception as e:
#                 self.log(f'Error writing data to HDFS: {e}')

#         else:
#             self.log(f'Failed to read file from WebHDFS. Status code: {response.status_code}')


# from streamparse import Bolt
# import requests
# from datetime import datetime
# import json
# from tika import parser
# import queue

# class Emit(Bolt):
#     outputs = ['output']

#     def initialize(self, storm_conf, context):
#         self.queue = queue.Queue()
#         self.is_processing = False

#     def process(self, tup):
#         document = tup.values[0]
#         document = json.loads(document)
#         hdfs_file_path = document.get('path')
#         email = document.get('email')

#         try:
#             if hdfs_file_path is not None:
#                 self.queue.put((hdfs_file_path, email))
#                 if not self.is_processing:
#                     self.process_queue()
#         except Exception as e:
#             self.log(f'Error processing document: {e}')

#     def write_data_to_hdfs(self, hdfs_file_path, email):
#         # WebHDFS endpoint and authentication
#         namenode_host = 'localhost'
#         namenode_port = '9870'
#         datanode_port = '9864'
#         username = 'ameen'
#         webhdfs_base_url = f'http://{namenode_host}:{namenode_port}/webhdfs/v1'
#         datanode_url = f'http://{namenode_host}:{datanode_port}/webhdfs/v1'
#         api_url = f'{webhdfs_base_url}{hdfs_file_path}/?op=OPEN'
#         # self.log(api_url)

#         session = requests.Session()
#         response = session.get(api_url)

#         if response.status_code == 200:
#             # self.log('Successfully read file from HDFS')
#             try:
#                 tika_server_url = 'http://localhost:9998'  # Replace with the appropriate Tika server URL
#                 parsed_data = parser.from_file(api_url, serverEndpoint=tika_server_url)
#                 text_content = parsed_data['content'].encode('utf-8').strip()
#                 # Get the current date
#                 current_date = datetime.now()
#                 year = current_date.strftime('%Y')
#                 month = current_date.strftime('%Y-%m')
#                 day = current_date.strftime('%Y-%m-%d')

#                 # Create the directory path based on the date hierarchy
#                 directory_path = f'{year}/{month}/{day}'

#                 # Create the WebHDFS URL to create the file
#                 create_url = f'{datanode_url}/{directory_path}/{str(email)}.txt?op=CREATE&user.name={username}&namenoderpcaddress={namenode_host}:9000&createflag=&createparent=true&overwrite=true'
#                 # self.log(create_url)

#                 # Write the data to HDFS using WebHDFS
#                 headers = {'Content-Type': 'application/octet-stream'}
#                 response = session.put(create_url, data=text_content, headers=headers)

#                 if response.status_code == 201:
#                     # self.log(f'Successfully wrote data to HDFS')

#                     tupvalues = json.dumps({"path": hdfs_file_path, "email": email, "directory": directory_path,
#                                              "namenode_host": namenode_host})

#                     self.emit([tupvalues])

#                 else:
#                     self.log(f'Failed to write file to WebHDFS. Status code: {response.status_code}')

#             except Exception as e:
#                 self.log(f'Error writing data to HDFS: {e}')

#         else:
#             self.log(f'Failed to read file from WebHDFS. Status code: {response.status_code}')

#     def process_queue(self):
#         self.is_processing = True
#         while not self.queue.empty():
#             hdfs_file_path, email = self.queue.get()
#             self.write_data_to_hdfs(hdfs_file_path, email)
#         self.is_processing = False

#     def process_tick(self, freq):
#         if not self.is_processing:
#             self.process_queue()

# from streamparse import Bolt
# import requests
# from datetime import datetime
# import json
# from tika import parser
# import asyncio

# class Emit(Bolt):
#     outputs = ['output']

#     def initialize(self, storm_conf, context):
#         self.documents = []

#     def process(self, tup):
#         document = tup.values[0]
#         document = json.loads(document)
#         hdfs_file_path = document.get('path')
#         email = document.get('email')

#         try:
#             if hdfs_file_path is not None:
#                 self.documents.append((hdfs_file_path, email))
#                 self.process_documents()
#         except Exception as e:
#             self.log(f'Error processing document: {e}')

#     def process_documents(self):
#         asyncio.run(self.process_documents_async())

#     async def process_documents_async(self):
#         while self.documents:
#             hdfs_file_path, email = self.documents.pop(0)
#             try:
#                 await self.write_data_to_hdfs(hdfs_file_path, email)
#             except Exception as e:
#                 self.log(f'Error processing document: {e}')

#     async def write_data_to_hdfs(self, hdfs_file_path, email):
#         # WebHDFS endpoint and authentication
#         namenode_host = 'localhost'
#         namenode_port = '9870'
#         datanode_port = '9864'
#         username = 'ameen'
#         webhdfs_base_url = f'http://{namenode_host}:{namenode_port}/webhdfs/v1'
#         datanode_url = f'http://{namenode_host}:{datanode_port}/webhdfs/v1'
#         api_url = f'{webhdfs_base_url}{hdfs_file_path}/?op=OPEN'
#         self.log(api_url)

#         session = requests.Session()
#         response = session.get(api_url)

#         if response.status_code == 200:
#             self.log('Successfully read file from HDFS')
#             try:
#                 tika_server_url = 'http://localhost:9998'  # Replace with the appropriate Tika server URL
#                 parsed_data = parser.from_file(api_url, serverEndpoint=tika_server_url)
#                 text_content = parsed_data['content'].encode('utf-8').strip()
#                 self.log("Text extraction done")

#                 # Get the current date
#                 current_date = datetime.now()
#                 year = current_date.strftime('%Y')
#                 month = current_date.strftime('%Y-%m')
#                 day = current_date.strftime('%Y-%m-%d')

#                 # Create the directory path based on the date hierarchy
#                 directory_path = f'{year}/{month}/{day}'

#                 # Create the WebHDFS URL to create the file
#                 create_url = f'{datanode_url}/{directory_path}/{str(email)}.txt?op=CREATE&user.name={username}&namenoderpcaddress={namenode_host}:9000&createflag=&createparent=true&overwrite=true'
#                 self.log(create_url)

#                 # Write the data to HDFS using WebHDFS
#                 headers = {'Content-Type': 'application/octet-stream'}
#                 response = session.put(create_url, data=text_content, headers=headers)

#                 if response.status_code == 201:
#                     self.log('Successfully wrote data to HDFS')

#                     tupvalues = json.dumps({"path": hdfs_file_path, "email": email, "directory": directory_path, "namenode_host": namenode_host})

#                     self.emit([tupvalues])

#                 else:
#                     self.log(f'Failed to write file to WebHDFS. Status code: {response.status_code}')

#             except Exception as e:
#                 self.log(f'Error writing data to HDFS: {e}')

#         else:
#             self.log(f'Failed to read file from WebHDFS. Status code: {response.status_code}')


# from streamparse import Bolt
# import requests
# from datetime import datetime
# import json
# from tika import parser
# import asyncio
# from collections import deque

# class Emit(Bolt):
#     outputs = ['output']

#     def initialize(self, storm_conf, context):
#         self.documents = deque()

#     def process(self, tup):
#         document = tup.values[0]
#         document = json.loads(document)
#         hdfs_file_path = document.get('path')
#         email = document.get('email')

#         try:
#             if hdfs_file_path is not None:
#                 self.documents.append((hdfs_file_path, email))
#                 self.process_documents()
#         except Exception as e:
#             self.log(f'Error processing document: {e}')

#     def process_documents(self):
#         asyncio.run(self.process_documents_async())

#     async def process_documents_async(self):
#         while self.documents:
#             hdfs_file_path, email = self.documents.popleft()
#             try:
#                 await self.write_data_to_hdfs(hdfs_file_path, email)
#             except Exception as e:
#                 self.log(f'Error processing document: {e}')

#     async def write_data_to_hdfs(self, hdfs_file_path, email):
#         # WebHDFS endpoint and authentication
#         namenode_host = 'localhost'
#         namenode_port = '9870'
#         datanode_port = '9864'
#         username = 'ameen'
#         webhdfs_base_url = f'http://{namenode_host}:{namenode_port}/webhdfs/v1'
#         datanode_url = f'http://{namenode_host}:{datanode_port}/webhdfs/v1'
#         api_url = f'{webhdfs_base_url}{hdfs_file_path}/?op=OPEN'
#         self.log(api_url)

#         session = requests.Session()
#         response = session.get(api_url)

#         if response.status_code == 200:
#             self.log('Successfully read file from HDFS')
#             try:
#                 tika_server_url = 'http://localhost:9998'  # Replace with the appropriate Tika server URL
#                 parsed_data = parser.from_file(api_url, serverEndpoint=tika_server_url)
#                 text_content = parsed_data['content'].encode('utf-8').strip()
#                 self.log("Text extraction done")

#                 # Get the current date
#                 current_date = datetime.now()
#                 year = current_date.strftime('%Y')
#                 month = current_date.strftime('%Y-%m')
#                 day = current_date.strftime('%Y-%m-%d')

#                 # Create the directory path based on the date hierarchy
#                 directory_path = f'{year}/{month}/{day}'

#                 # Create the WebHDFS URL to create the file
#                 create_url = f'{datanode_url}/{directory_path}/{str(email)}.txt?op=CREATE&user.name={username}&namenoderpcaddress={namenode_host}:9000&createflag=&createparent=true&overwrite=true'
#                 self.log(create_url)

#                 # Write the data to HDFS using WebHDFS
#                 headers = {'Content-Type': 'application/octet-stream'}
#                 response = session.put(create_url, data=text_content, headers=headers)

#                 if response.status_code == 201:
#                     self.log('Successfully wrote data to HDFS')

#                     tupvalues = json.dumps({"path": hdfs_file_path, "email": email, "directory": directory_path, "namenode_host": namenode_host})

#                     self.emit([tupvalues])

#                 else:
#                     self.log(f'Failed to write file to WebHDFS. Status code: {response.status_code}')

#             except Exception as e:
#                 self.log(f'Error writing data to HDFS: {e}')

#         else:
#             self.log(f'Failed to read file from WebHDFS. Status code: {response.status_code}')




# from streamparse import Bolt
# import requests
# from datetime import datetime
# import json
# from tika import parser
# import asyncio
# from collections import deque

# class Emit(Bolt):
#     outputs = ['output']

#     def initialize(self, storm_conf, context):
#         self.namenode_host = 'localhost'
#         self.namenode_port = '9870'
#         self.datanode_port = '9864'
#         self.username = 'ameen'
#         self.webhdfs_base_url = f'http://{self.namenode_host}:{self.namenode_port}/webhdfs/v1'
#         self.datanode_url = f'http://{self.namenode_host}:{self.datanode_port}/webhdfs/v1'
#         self.documents = deque()
#         self.hdfs_file_path = None
#         self.email = None

#     def process(self, tup):
#         self.document = tup.values[0]
#         self.document = json.loads(self.document)
#         self.hdfs_file_path = self.document.get('path')
#         self.email = self.document.get('email')

#         try:
#             if self.hdfs_file_path is not None:
#                 self.documents.append((self.hdfs_file_path, self.email))
#                 self.process_documents()
#         except Exception as e:
#             self.log(f'Error processing document: {e}')
#             self.documents.append((self.hdfs_file_path, self.email))
#             self.log('appended')

#     def process_documents(self):
#         asyncio.run(self.process_documents_async())

#     async def process_documents_async(self):
#         while self.documents:
#             self.hdfs_file_path, self.email = self.documents.popleft()
#             await self.Text_Conversion()
            

#     async def Text_Conversion(self):
#         # WebHDFS endpoint and authentication
#         api_url = f'{self.webhdfs_base_url}{self.hdfs_file_path}/?op=OPEN'
#         self.log(api_url)

#         session = requests.Session()
#         response = session.get(api_url)

#         if response.status_code == 200:
#             self.log('Successfully read file from HDFS')
#             tika_server_url = 'http://localhost:9998'  # Replace with the appropriate Tika server URL
#             parsed_data = parser.from_file(api_url, serverEndpoint=tika_server_url)
#             text_content = parsed_data['content'].encode('utf-8').strip()
#             self.text_content = text_content
#             self.log("Text extraction done")

#             try:
#                 await self.write_to_hdfs()
#             except Exception as e:
#               self.log(f'Error writing data to HDFS: {e}')
#               self.documents.append((self.hdfs_file_path, self.email))
#               self.log('appended back')


#     async def write_to_hdfs(self):
#             # Get the current date
#             current_date = datetime.now()
#             year = current_date.strftime('%Y')
#             month = current_date.strftime('%Y-%m')
#             day = current_date.strftime('%Y-%m-%d')

#             # Create the directory path based on the date hierarchy
#             directory_path = f'{year}/{month}/{day}'

#             # Create the WebHDFS URL to create the file
#             create_url = f'{self.datanode_url}/{directory_path}/{str(self.email)}.txt?op=CREATE&user.name={self.username}&namenoderpcaddress={self.namenode_host}:9000&createflag=&createparent=true&overwrite=true'

#             # Write the data to HDFS using WebHDFS
#             headers = {'Content-Type': 'application/octet-stream'}
#             response = requests.put(create_url, data=self.text_content, headers=headers)

#             if response.status_code == 201:
#                 self.log('Successfully wrote data to HDFS')
#                 tupvalues = json.dumps({"path": self.hdfs_file_path, "email": self.email, "directory": directory_path, "namenode_host": self.namenode_host})
#                 self.emit([tupvalues])



from streamparse import Bolt
import requests
from datetime import datetime
import json
from tika import parser
from collections import deque

class Emit(Bolt):
    outputs = ['output']

    def initialize(self, storm_conf, context):
        self.namenode_host = 'localhost'
        self.namenode_port = '9870'
        self.datanode_port = '9864'
        self.username = 'ameen'
        self.webhdfs_base_url = f'http://{self.namenode_host}:{self.namenode_port}/webhdfs/v1'
        self.datanode_url = f'http://{self.namenode_host}:{self.datanode_port}/webhdfs/v1'
        self.documents = deque()
        self.hdfs_file_path = None
        self.email = None

    def process(self, tup):
        self.document = tup.values[0]
        self.document = json.loads(self.document)
        self.hdfs_file_path = self.document.get('path')
        self.email = self.document.get('email')

        try:
            if self.hdfs_file_path is not None:
                self.documents.append((self.hdfs_file_path, self.email))
                self.process_documents()
        except Exception as e:
            self.log(f'Error processing document: {e}')
            self.documents.append((self.hdfs_file_path, self.email))
            self.log('appended')

    def process_documents(self):
        while self.documents:
            self.hdfs_file_path, self.email = self.documents.popleft()
            self.text_conversion()

    def text_conversion(self):
        # WebHDFS endpoint and authentication
        api_url = f'{self.webhdfs_base_url}{self.hdfs_file_path}/?op=OPEN'
        self.log(api_url)

        session = requests.Session()
        response = session.get(api_url)

        if response.status_code == 200:
            self.log('Successfully read file from HDFS')
            tika_server_url = 'http://localhost:9998'  # Replace with the appropriate Tika server URL
            parsed_data = parser.from_file(api_url, serverEndpoint=tika_server_url)
            text_content = parsed_data['content'].encode('utf-8').strip()
            self.text_content = text_content
            self.log("Text extraction done")
            
            try:
                self.write_to_hdfs()
            except Exception as e:
                self.log(f'Error writing data to HDFS: {e}')
                self.documents.append((self.hdfs_file_path, self.email))
                self.log('appended back')

    def write_to_hdfs(self):
        # Get the current date
        current_date = datetime.now()
        year = current_date.strftime('%Y')
        month = current_date.strftime('%Y-%m')
        day = current_date.strftime('%Y-%m-%d')

        # Create the directory path based on the date hierarchy
        directory_path = f'{year}/{month}/{day}'

        # Create the WebHDFS URL to create the file
        create_url = f'{self.datanode_url}/{directory_path}/{str(self.email)}.txt?op=CREATE&user.name={self.username}&namenoderpcaddress={self.namenode_host}:9000&createflag=&createparent=true&overwrite=true'

        # Write the data to HDFS using WebHDFS
        headers = {'Content-Type': 'application/octet-stream'}
        response = requests.put(create_url, data=self.text_content, headers=headers)

        if response.status_code == 201:
            self.log('Successfully wrote data to HDFS')
            tupvalues = json.dumps({"path": self.hdfs_file_path, "email": self.email, "directory": directory_path, "namenode_host": self.namenode_host})
            self.emit([tupvalues])

       
