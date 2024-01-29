import argparse
import logging
import os
import random
import sys
import threading
import socket
import time
from fastapi.responses import JSONResponse
import requests
import uvicorn

from fastapi import FastAPI, HTTPException, Response, status
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from requests import Session
#import memcache
#import redis


class CassandraSessionManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(CassandraSessionManager, cls).__new__(cls)
                cls._instance.cluster = None
                cls._instance.session = None
        return cls._instance

    def connect(self, hosts, port):
        if self.session is not None:
            return self.session

        try:
            auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
            self.cluster = Cluster(contact_points=hosts, port=port, auth_provider=auth_provider)
            self.session = self.cluster.connect()
        except Exception as e:
            logging.error(f"Error connecting to Cassandra: {e}")
            raise

        return self.session

    def shutdown(self):
        if self.cluster is not None:
            self.cluster.shutdown()
        if self.session is not None:
            self.session.shutdown()

        self.cluster = None
        self.session = None
        




                


#follower
