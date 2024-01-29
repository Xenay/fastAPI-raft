import asyncio
import threading
import argparse
import logging
import os
import random
import sys
import threading
import socket
import time
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import requests
import uvicorn
from typing import List, Dict, Optional
from fastapi import FastAPI, HTTPException, Response, status
from requests import Session
from raft.heartbeatMonitor import HeartbeatMonitor
from raft.log import LogEntry
from worker import write_to_file, getValue, putValue

messages_to_mass_write = ""

class AppendEntriesRequest(BaseModel):
    term: int
    leader_id: int
    prev_log_index: Optional[int] = None
    prev_log_term: Optional[int] = None
    entries: List[Dict]
    leader_commit: int

class Node:
    def __init__(self, ip, port, all_nodes, cassandra_hosts, app):
        self.ip = ip
        self.port = port
        self.app = app
        self.active_requests = 0
        self.active_requests_lock = threading.Lock()
        self.http_session = Session()
        self.node_id = port
        self.nodeIsAlive = True
        #self.redis_client = redis.Redis(host='localhost', port=6379, db=0)  # Adjust the host and port accordingly
        #self.memcached_client = memcache.Client(['127.0.0.1:11211'], debug=0)  # Adjust host and port if needed
        self.cassandra_hosts = cassandra_hosts  # Store the Cassandra hosts
        self.all_nodes = all_nodes
        #self.cache = {}  # Simple cache
        #self.cache_size = 1000
        self.state = [state for state in all_nodes if state['port'] == self.port][0]['state']
        self.last_heartbeat_time = time.time()
        #election
        self.election_timeout = random.randint(1000, 2000) / 100
        self.term = 1  # Election term
        self.voted_for = None
        self.deadNode = 0
        #log
        self.log = []
        self.commit_index = 1
        self.nextIndex = {node['port']: len(self.log) for node in self.all_nodes}
        self.monitor = HeartbeatMonitor(self.all_nodes, self.commit_index, self.node_id, self.term, self.log, self.nextIndex)
        
        @self.app.get("/heartbeat")
        async def receive_heartbeat():
            self.last_heartbeat_time = time.time()
            return {"status": "heartbeat received", "port": self.port}
                
        @self.app.get("/get/{key}")
        async def get(key: str):
                with self.active_requests_lock:  
                    self.active_requests += 1
                print(self.active_requests)
                redirect_node = self.should_redirect()
                if redirect_node:
            # Redirect request to the follower node
                    response = requests.get(f"http://{redirect_node['ip']}:{redirect_node['port']}/get/{key}")
                    print(f"got response from {redirect_node['port']}")
                    return response.json()
                else:
                #cached_value = self.memcached_client.get(key)
            #cached_value = self.redis_client.get(key)
            #if cached_value:
                #return cached_value.decode('utf-8') 
                
                    
                    
                    # Load balancing: redirect if load is high
                    #if self.should_redirect():  # Threshold for load
                    
                   
                    try:
                        ''' DB IMPLEMENTATION '''
                        value = await getValue(key)
                        if value != "Key not found":
                            self.active_requests -= 1
                            return {"key": key, "value": value}
                        return {"error": "Key not found"}
                                #self.memcached_client.set(key, value)
                                #self.redis_client.set(key, value)
                    except Exception as e:
                        print(f"Primary Cassandra instance failed: {e}")
                        # Switch to the secondary Cassandra instance
                                
                with self.active_requests_lock:    
                    self.active_requests -= 1
                self.append_new_entry_and_replicate("get")
                return {"error": "Key not found"}

        @self.app.post("/post/{key}")
        async def post(key: str, value: str):
        # Check if the key already exists
            #if not self.is_node_alive():
                #return await self.redirect_request(key)
            ''' DB IMPLEMENTATION '''
            start_writing = time.time()
            #print(start_writing)
            await write_to_file(key,value)
            #print(time.time(), time.time() - start_writing)
            print(f"Time needed to write this message to file was {time.time() - start_writing}")
            ''' DEBUG '''
            if self.state == 'leader':
                self.active_requests+=1
                self.active_requests-=1
                command = f"post {key} {value}"  
                # The command to be replicated
                self.append_new_entry_and_replicate(command)
                return {"message": "Write request processed and replicated"}     
            self.active_requests-=1
            #self.update_cache(key, value)
            return {"message": "New key-value pair created successfully"}
        
        @self.app.post("/turnToFollower")
        async def turnToFollower(updated_nodes: List[Dict]):
            self.all_nodes = updated_nodes
            self.state = "follower"
            return {"message": "State and nodes updated successfully"}

        @self.app.put("/put/{key}")
        async def put(key: str, value: str):
            #if not self.is_node_alive():
                #return await self.redirect_request(key)
            self.active_requests+=1
            await putValue(key, value)
            command = f"put {key} {value}"  
            self.append_new_entry_and_replicate(command)
            self.active_requests-=1
            return {"message": "Value stored successfully"}

        @self.app.delete("/delete/{key}")
        async def delete(key: str):
            self.active_requests+=1
            self.active_requests-=1
            command = f"delete {key}"  
            self.append_new_entry_and_replicate(command)
            return {"message": "Key deleted successfully"}

        @self.app.get("/health")
        async def health_check():
            return {"status": "alive"}
        
        @self.app.get("/shutdown")
        async def shutdown():
            # Shutdown the Uvicorn server
            uvicorn_server = getattr(self.app, 'server', None)
            if uvicorn_server:
                await uvicorn_server.shutdown()
            return {"status": "shutting down"}
        
        @self.app.get("/vote")
        async def vote(candidate_id: str, term: int):
            if term > self.term and (self.voted_for is None or self.voted_for == candidate_id):
                self.voted_for = candidate_id
                self.term = term
                return {"vote_granted": True}
            return {"vote_granted": False}

        @self.app.get("/debug")
        async def debug():
            if self.state ==  "leader":
                self.append_new_entry_and_replicate("start")
            return {"state": self.state, "all nodes": self.all_nodes, "log": self.log }
        
        @app.post("/append_entries")
        async def append_entries(request: AppendEntriesRequest):
            print("Received request data:", request.dict())
            # Check if the term is outdated
            #if request.term < self.term:
                #return {"success": False, "term": self.term, "error": "Outdated term"}

            # # Check if the previous log index is out of bounds
            # if request.prev_log_index >= len(self.log) or request.prev_log_index < -1:
            #     return {"success": False, "term": self.term, "error": "prev_log_index out of bounds"}

            # # Check if the term at the previous log index matches
            #
            #if request.prev_log_index != -1 and self.log[request.prev_log_index].term != request.prev_log_term:
               #return {"success": False, "term": self.term, "error": "Log term mismatch"}
            #buggy
            # if request.prev_log_index != -1:
            #     if request.prev_log_index > len(self.log):
            #         print("condition 1")
            #if leader index than leader,
            #         return {"success": False, "term": self.term, "error": "prev_log_index out of bounds"}
            
            if (request.leader_commit > (self.commit_index + 1)) and (len(self.log) > 0):
                    print("leader_commint" , request.leader_commit)
                    print("commit index" , self.commit_index)
                    self.commit_index = min(request.leader_commit, len(self.log))
                    return {"success": False, "term": self.term, "error": "Log index mismatch", "mismatch_index": request.prev_log_index}
           
            for entry in request.entries:
                new_log_entry = LogEntry(index=entry['index'], term=entry['term'], command=entry['command'])
                # Check if new entry's index is greater than the last index in the log
                if not self.log or new_log_entry.index > self.log[-1].index:
                    self.log.append(new_log_entry)
            
            self.commit_index +=1
            return {"success": True, "term": self.term, "last_log_index": len(self.log) - 1}

    def start_server(self):
        uvicorn.run(self.app, host=self.ip, port=self.port, log_level="info")
  
    def start_election(self):
        self.term += 1
        self.state = "candidate"
        self.voted_for = self.port
        votes = 1

        for node in self.all_nodes:
            if node['port'] != self.port:
                try:
                    response = self.http_session.get(f"http://{node['ip']}:{node['port']}/vote",
                                                    params={"candidate_id": self.node_id, "term": self.term})
                    if response.status_code == 200 and response.json().get("vote_granted"):
                        votes += 1
                except requests.exceptions.RequestException as e:
                    print(f"Error contacting node {node['port']}: {e}")

        if votes > (len(self.all_nodes)-self.deadNode) // 2:
            self.state = "leader"
            #self.nextIndex +=1
            '''CHATGPT SOLUTION'''
            for node in self.all_nodes:
                if node['port'] == self.port:
                    node['state'] = 'leader'
                else:
                    node['state'] = 'follower'  
                self.nextIndex[node['port']] = len(self.log)
                    
            self.start_heartbeat()
            ###WIP SOLUTION
            # monitor = HeartbeatMonitor(self.all_nodes, self.commit_index, self.node_id, self.term, self.log, self.nextIndex)
            # monitor_thread = threading.Thread(target=monitor.send_heartbeats)
            # monitor_thread.start()
            print(f"Elected as leader for term {self.term}")
            for node in self.all_nodes:
                if node['port'] != self.port:
                        self.http_session.get(f"http://{node['ip']}:{node['port']}/turnToFollower", json=self.all_nodes)
                        

    def check_if_leader_alive(self):
        print((time.time() - self.last_heartbeat_time))
        if (time.time() - self.last_heartbeat_time) > self.election_timeout and (self.state != "leader"):
            print("timeout")
            #self.start_election()
            return False
        return True
    
    def start_heartbeat(self):
        #monitor = HeartbeatMonitor(self.all_nodes, self.commit_index, self.node_id, self.term, self.log, self.nextIndex)
        monitor_thread = threading.Thread(target=self.monitor.send_heartbeats)
        heartbeat_interval = 0.5  # Half a second, adjust as needed
        #heartbeat_thread = threading.Thread(target=self.monitor.send_append_entries)
        #heartbeat_thread.start()
        monitor_thread.start()
        #update_thread = threading.Thread(target=self.updateDataToSend)
        #update_thread.start()
        
    def updateDataToSend(self):      
        self.monitor.nodes = self.all_nodes
        self.monitor.commit_index= self.commit_index
        self.monitor.leader_id= self.node_id
        self.monitor.term= self.term
        self.monitor.next_index = self.nextIndex
        self.monitor.log = self.log 
        self.monitor.leader_index = self.commit_index
        
    def stop_heartbeat(self):
        if self.heartbeat_thread:
            self.heartbeat_thread.cancel()
    
    def append_new_entry_and_replicate(self, command):
        # Append a new entry to the leader's log
        new_log_entry = LogEntry(index=len(self.log)+1, term=self.term, command=command)
        
        #self.log.append(new_log_entry)
        # Update nextIndex for all followers to the new length of the log
        # for node in self.all_nodes:
        #     if node['state'] != 'leader':
        #         print("before: ", self.nextIndex)
        #         self.nextIndex[node['port']] = len(self.log)
        #         print("after: ", self.nextIndex)
        # Trigger log replication to the follower nodes
        
        self.replicate_log_to_followers(new_log_entry)
        
        
    def replicate_log_to_followers(self,command):
        #monitor2 = HeartbeatMonitor(self.all_nodes, self.commit_index, self.node_id, self.term, self.log, self.nextIndex)
        self.monitor.log.append(command)
        #self.nextIndex[self.port] += 1
        self.monitor.next_index[self.port] += 1
        self.monitor.send_append_entries()
        self.updateDataToSend()
        #self.commit_index+=1
    
    #redirecting load WIP
    def should_redirect(self):
        with self.active_requests_lock:
            if self.active_requests > 100:
                return self.find_least_loaded_follower()
        return None
    
    def find_least_loaded_follower(self):
        least_loaded_node = None
        min_load = float('inf')
        for node in self.all_nodes:
            if node['port'] != self.port and node['state'] == 'follower':
                # Assume each node has a way to report its load, e.g., via an API endpoint /load
                response = requests.get(f"http://{node['ip']}:{node['port']}/load")
                print(f"sending request to port: {node['port']} ------------------------------------------------------------------------------------------------")
                if response.status_code == 200:
                    load = response.json().get('load')
                    if load < min_load:
                        min_load = load
                        least_loaded_node = node
        return least_loaded_node