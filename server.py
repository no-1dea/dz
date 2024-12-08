import time
import random
import threading
import stat
import difflib
import unittest
import requests
import sys
import os
import socketserver
import logging
from flask import Flask, jsonify, request
from collections import defaultdict
import socket
import subprocess


ELECTION_TIMEOUT_MIN = 4
ELECTION_TIMEOUT_MAX = 10
HEARTBEAT_INTERVAL = 1

SERVER_ID = int(
    os.getenv("SERVER_ID", 1)
)

SERVER_ADDRESSES = {
    1: "http://raft-server-1:5001",
    2: "http://raft-server-2:5002",
    3: "http://raft-server-3:5003",
    4: "http://raft-server-4:5004",
    5: "http://raft-server-5:5005",
}


def setup_logging():
    logger = logging.getLogger("RaftServer")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    return logger

logger = setup_logging()

class RaftServer:

    def __init__(self, server_id: int):
        self.server_id = server_id
        self.state = "follower"
        self.leader_id = None
        self.last_heartbeat_time = time.time()
        self.term = 0
        self.votes_by_term = dict()
        self.alive = True

        self.change_log = dict()

        self.buf = []

        self.versions = dict()

        self.current_term = 0
        self.voted_for = None
        self.log = list()
        self.commit_index = -1
        self.last_applied = -1
        self.next_index = {}
        self.match_index = {}

        for server_id in SERVER_ADDRESSES:
            self.versions[server_id] = 0

        self.election_timeout = (
            ELECTION_TIMEOUT_MIN + self.server_id * 3
        )
        self.election_timeout_start_time = time.time()

        self.app = Flask(__name__)
        self.initialize_routes()
    def initialize_routes(self):
        routes = [
            ("/heartbeat", self.heartbeat, ["POST"]),
            ("/vote", self.vote, ["POST"]),
            ("/status", self.status, ["GET"]),
            ("/turnoff", self.turnoff, ["GET"]),
            ("/turnon", self.turnon, ["GET"]),
            ("/get_data", self.get_data, ["GET"]),
            ("/put_data", self.put_data, ["PUT"]),
            ("/post_data", self.post_data, ["POST"]),
            ("/delete_data", self.delete_data, ["DELETE"]),
            ("/head_data", self.head_data, ["HEAD"]),
            ("/update_data", self.update_data, ["PATCH"]),
            ("/repl", self.repl, ["POST"]),
        ]
        for rule, view_func, methods in routes:
            self.app.add_url_rule(rule, view_func.__name__, view_func, methods=methods)


    def repl(self):
        data = request.get_json()
        leader_id = data.get("leader_id")
        term = data.get("term")

        if self.term > term:
            return jsonify({"status": "bad"})

        if self.term <= term:
            self.state = "follower"
            self.term = term

        if leader_id is not None:
            self.last_heartbeat_time = time.time()
            self.leader_id = leader_id

        if "change_log" in data:
            self.buf = self.log.copy()
            for el in data.get("change_log"):
                self.buf.append(el)
            return jsonify({"status": "ack"})
            
        if "commit" in data:
            for el in self.buf:
                self.log.append(el)
                if el["type"] == "put":
                    self.change_log[el["key"]] = el["value"]
                if el["type"] == "delete":
                    del self.change_log[el["key"]]
            
            return jsonify({"status": "ok"})
        
        return jsonify({"status": "bad"})
        
        

    def get_data(self):
        data = request.get_json()
        key = data.get("key")

        if self.state != "leader":
            try:
                response = requests.get(
                    f"{SERVER_ADDRESSES[self.leader_id]}/get_data",
                    json={"key": key},
                    timeout=1
                )
                return jsonify(
                    response.json()
                )
            except requests.RequestException as e:
                return jsonify(
                    {
                        "status": "error", 
                        "message": str(e)
                    }
                )
        else:
            key_ver = -1
            i = 0
            #might be replaced with Treap to obtain log(n) complexity, or maintain 
            #in the db along with the data the postion in log journal to get O(1)
            for el in self.log[::-1]:
                if key == el["key"]:
                    key_ver = len(self.log) - i
                    break
                i += 1
            for server_id in SERVER_ADDRESSES:
                if self.versions[server_id] > key_ver:
                    return jsonify({"id" : server_id}), 302
            return jsonify(
                {
                    "key": key, 
                    "value": self.change_log.get(key)
                }
            )


    def put_data(self):
        data = request.get_json()
        key = data.get("key")
        value = data.get("value")

        if self.state != "leader":
            try:
                response = requests.post(
                    f"{SERVER_ADDRESSES[self.leader_id]}/put_data",
                    json={"key": key, "value": value},
                    timeout=1
                )
                return jsonify(response.json())
            except requests.RequestException as e:
                return jsonify(
                    {
                        "status": "error",
                        "message": str(e)
                    }
                )
        else:
            self.log.append({'type' : "put", "key": key, "value": value})
            self.change_log[key] = value
            return jsonify({"status": "ok"})


    def post_data(self):
        data = request.get_json()
        key = data.get("key")
        value = data.get("value")

        if self.state != "leader":
            try:
                response = requests.post(
                    f"{SERVER_ADDRESSES[self.leader_id]}/post_data",
                    json={"key": key, "value": value},
                    timeout=1
                )
                return jsonify(
                    response.json()
                )
            except requests.RequestException as e:
                return jsonify(
                    {
                        "status": "error",
                        "message": str(e)
                    }
                )
        else:
            self.change_log[key] = value
            self.log.append({'type' : "put", "key": key, "value": value})
            return jsonify({"status": "ok"})


    def delete_data(self):
        data = request.get_json()
        key = data.get("key")

        if self.state != "leader":
            try:
                response = requests.delete(
                    f"{SERVER_ADDRESSES[self.leader_id]}/delete_data",
                    json={"key": key},
                    timeout=1
                )
                return jsonify(response.json())
            except requests.RequestException as e:
                return jsonify(
                    {
                        "status": "error", 
                        "message": str(e)
                    }
                )
        else:
            if key in self.change_log:
                self.log.append({'type' : "delete", "key": key})
                del self.change_log[key]
                return jsonify({"status": "ok"})
            return jsonify(
                {
                    "status": "error", 
                    "message": "Key not found"
                }
            )


    def head_data(self):
        data = request.get_json()
        key = data.get("key")

        if self.state != "leader":
            try:
                response = requests.head(
                    f"{SERVER_ADDRESSES[self.leader_id]}/head_data",
                    json={
                        "key": key
                    },
                    timeout=1
                )
                return jsonify(response.headers)
            except requests.RequestException as e:
                return jsonify(
                    {
                        "status": "error", 
                        "message": str(e)
                    }
                )
        else:
            if key in self.change_log:
                return jsonify(
                    {
                        "status": "exists"
                    }
                )
            return jsonify(
                {
                    "status": "not found"
                }
            )


    def update_data(self):
        data = request.get_json()
        key = data.get("key")
        value = data.get("value")
        old = data.get("old")

        if self.state != "leader":
            try:
                response = requests.patch(
                    f"{SERVER_ADDRESSES[self.leader_id]}/update_data",
                    json={
                        "key": key, 
                        "value": value
                    },
                    timeout=1
                )
                return jsonify(response.json())
            except requests.RequestException as e:
                return jsonify(
                    {
                        "status": "error", 
                        "message": str(e)
                    }
                )
        else:
            if key in self.change_log:
                if self.change_log[key] != old:
                    return jsonify(
                        {
                            "status": "error", 
                            "message": "Value has been changed"
                        }
                    )
                self.change_log[key] = value
                self.log.append({'type' : "put", "key": key, "value": value})
                cnt = 0
                for server_id, url in SERVER_ADDRESSES.items():
                    if server_id != self.server_id:
                        try:
                            response = requests.post(
                                f"{url}/heartbeat",
                                json={
                                    "leader_id": self.server_id,
                                    "term": self.term,
                                },
                                timeout=1
                            )
                            data = response.json()
                            cur_len = data.get("cur_len")
                            if cur_len < len(self.log):
                                if cur_len == 0:
                                    cur_len += 1
                                response = requests.post(
                                f"{url}/repl",
                                json={
                                    "leader_id": self.server_id,
                                    "term": self.term,
                                    "change_log": list(self.log[cur_len - 1:])
                                },
                                timeout=1
                                )
                                #self.versions[server_id] = response.json().get("cur_len")
                                if response.json().get("status") == "ack":
                                    cnt += 1
                        except requests.exceptions.RequestException:
                            pass
                if cnt > len(SERVER_ADDRESSES) // 2:
                    for server_id, url in SERVER_ADDRESSES.items():
                        if server_id != self.server_id:
                            try:
                                response = requests.post(
                                    f"{url}/repl",
                                    json={
                                        "leader_id": self.server_id,
                                        "term": self.term,
                                        "commit": "yes"
                                    },
                                    timeout=1
                                )
                            except requests.exceptions.RequestException:
                                pass
                    logger.info(f"Commited {len(self.log[cur_len - 1:])} entries")
                    return jsonify(
                        {
                            "status": "ok"
                        }
                    )
                else:
                    del self.change_log[key]
                    self.log.pop()
                    return jsonify(
                        {
                            "status": "error", 
                            "message": "Not enough servers ack"
                        }
                    )
            return jsonify(
                {
                    "status": "error", 
                    "message": "Key not found"
                }
            )


    def turnon(self):
        self.alive = True
        logger.info(f"is alive now in term: {self.term}")
        return jsonify(
            {
                "status": "ok"
            }
        )


    def turnoff(self):
        self.alive = False
        logger.info(f"is dead now")
        return jsonify(
            {
                "status": "ok"
            }
        )


    def send_heartbeat(self):
        while True:
            self.deadimitation()

            if self.state == "leader":
                for server_id, url in SERVER_ADDRESSES.items():
                    if server_id != self.server_id:
                        try:
                            response = requests.post(
                                f"{url}/heartbeat",
                                json={
                                    "leader_id": self.server_id,
                                    "term": self.term,
                                },
                                timeout=1
                            )
                            data = response.json()
                            cur_len = data.get("cur_len")
                            if cur_len < len(self.change_log):
                                if cur_len == 0:
                                    cur_len += 1
                                response = requests.post(
                                f"{url}/heartbeat",
                                json={
                                    "leader_id": self.server_id,
                                    "term": self.term,
                                    "change_log": list(self.log[cur_len - 1:])
                                },
                                timeout=1
                                )
                                self.versions[server_id] = response.json().get("cur_len")
                        except requests.exceptions.RequestException:
                            pass
                self.last_heartbeat_time = time.time()
            time.sleep(HEARTBEAT_INTERVAL)


    def start_election(self):
        """Запуск выборов, если сервер стал кандидатом."""

        votes = 0

        self.term = self.term + 1
        for server_id, url in SERVER_ADDRESSES.items():
            try:
                response = requests.post(
                    f"{url}/vote",
                    json={
                        "candidate_id": self.server_id,
                        "term": self.term
                    },
                    timeout=1
                )
                if response.json().get("vote_granted"):
                    votes += 1
            except requests.exceptions.RequestException as e:
                print(f'SERVER #{server_id} failed: {e}')
                pass

        if votes > len(SERVER_ADDRESSES) // 2:
            self.state = "leader"
            self.leader_id = self.server_id
            logger.info(f"Server {self.server_id} is elected as leader!")

        self.last_heartbeat_time = time.time()


    def election_check(self):

        while True:
            self.deadimitation()

            if time.time() - self.last_heartbeat_time > self.election_timeout:
                if self.state != "leader":
                    logger.info(f"Server {self.server_id} starts election!")
                    self.start_election()

                self.election_timeout_start_time = time.time()

            self.log_stats()
            time.sleep(1)


    def log_stats(self):
        logger.info(
            f'TERM: {self.term}, ID: {self.server_id}, State: {self.state}, ChangeLog: {self.log}, DB: {self.change_log.items()}, Buf: {self.buf}'
        )
        return


    def deadimitation(self):
        while not self.alive:
            time.sleep(0.5)


    def heartbeat(self):
        self.deadimitation()

        data = request.get_json()
        leader_id = data.get("leader_id")
        term = data.get("term")

        if self.term > term:
            return jsonify({"status": "bad"})

        if self.term <= term:
            self.state = "follower"
            self.term = term

        if leader_id is not None:
            self.last_heartbeat_time = time.time()
            self.leader_id = leader_id

        if "change_log" in data:
            for el in  data.get("change_log"):
                self.log.append(el)
                if el["type"] == "put":
                    self.change_log[el["key"]] = el["value"]
                if el["type"] == "delete":
                    del self.change_log[el["key"]]

        #self.change_log = data.get("change_log")

        return jsonify({"status": "ok", "cur_len" : len(self.log)})


    def vote(self):
        self.deadimitation()

        data = request.get_json()
        candidate_id = data.get("candidate_id")
        term = data.get("term")

        if term > self.term:
            self.term = term

        if self.server_id == candidate_id:
            logger.info(f"Server {self.server_id} votes for candidate {candidate_id}")
            return jsonify(
                {
                    "vote_granted": True
                }
            )

        if self.state == "follower":
            if term in self.votes_by_term:
                return jsonify({"vote_granted": False})

            self.last_heartbeat_time = time.time()
            self.votes_by_term[term] = candidate_id
            logger.info(f"Server {self.server_id} votes for candidate {candidate_id}")

            return jsonify(
                {
                    "vote_granted": True
                }
            )

        return jsonify(
            {
                "vote_granted": False
            }
        )


    def status(self):
        return jsonify(
            {
                "state": self.state,
                "leader_id": self.leader_id,
                "term": self.term
            }
        )


    def run(self):
        threading.Thread(
            target=self.send_heartbeat, 
            daemon=True
        ).start()

        threading.Thread(
            target=self.election_check, 
            daemon=True
        ).start()

        log = logging.getLogger('werkzeug')
        log.setLevel(logging.WARNING)

        self.app.run(
            host="0.0.0.0", 
            port=5000 + self.server_id, 
            threaded=True
        )


if __name__ == "__main__":
    print(
        'SERVER IS STARTING', 
        file=sys.stderr
    )
    raft_server = RaftServer(SERVER_ID)
    raft_server.run()
