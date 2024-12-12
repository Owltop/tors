import os
import time
import random
import json
import threading
import requests
from flask import Flask, request, jsonify
import math
import uuid

app = Flask(__name__)

class RaftNode:
    def __init__(self, node_id, cluster):
        self.node_id = node_id
        self.cluster = cluster
        
        # Состояние узла
        self.current_term = 0   # Храним на диске
        self.voted_for = None   # Храним на диске
        self.log = []           # Храним на диске (журнал операций)
        self.commit_length = 0  # Храним на диске
        self.data = {}          # Храним на диске, тут уже закомиченные данные

        # Временные переменные
        self.current_role = "follower"  # follower, candidate, leader
        self.current_leader = None
        self.votes_received = {}
        self.sent_length = dict()
        self.acked_length = dict()

        # Синхронизация
        self.election_timeout = random.uniform(1.5, 7.0)
        self.last_heartbeat = time.time()
        self.heartbeat_interval = 0.5

        # Файлы для хранения данных Raft
        self.term_file = f"{node_id}_term.json"
        self.log_file = f"{node_id}_log.json"
        self.data_file = f"{node_id}_data.json"

        # Загрузка данных с диска
        self.load_from_disk()

        # Запуск таймера
        threading.Thread(target=self.election_timer, daemon=True).start()
        threading.Thread(target=self.send_heartbeat, daemon=True).start()

    def load_from_disk(self):
        """Загрузка данных Raft и key-value хранилища с диска при старте узла."""
        if os.path.exists(self.term_file):
            with open(self.term_file, 'r') as file:
                term_data = json.load(file)
                self.current_term = term_data.get("currentTerm", 0)
                self.voted_for = term_data.get("votedFor", None)
                self.commit_length = term_data.get("commitLength", 0)

        if os.path.exists(self.log_file):
            with open(self.log_file, 'r') as file:
                self.log = json.load(file)

        if os.path.exists(self.data_file):
            with open(self.data_file, 'r') as file:
                self.data = json.load(file)

        print(f"[Node {self.node_id}] Data loaded from disk. CurrentTerm: {self.current_term}, LogLength: {len(self.log)}, Data: {self.data}")

    def save_to_disk(self):
        """Сохраняет `currentTerm`, `votedFor` и `log` на диск."""
        with open(self.term_file, 'w') as file:
            json.dump({"currentTerm": self.current_term, "votedFor": self.voted_for, "commitLength": self.commit_length}, file)

        with open(self.log_file, 'w') as file:
            json.dump(self.log, file)

        with open(self.data_file, 'w') as file:
            json.dump(self.data, file)

    #############################
    # Клиентский CRUD-запрос к лидеру
    #############################

    def handle_client_action(self, key, value, action):
        """Обработка запроса клиента на лидере."""
        if self.current_role != "leader":
            return self.current_leader

        entry = {"term": self.current_term, "key": key, "value": value, "action": action}
        self.log.append(entry)
        self.save_to_disk()
        self.acked_length[self.node_id] = len(self.log)

        for follower in self.cluster:
            if follower != self.node_id:
                self.replicate_log(self.node_id, follower)
        return -1
    
    def handle_cas(self, key, old_value, new_value):
        if self.current_role != "leader":
            return False, self.current_leader
        
        guid = str(uuid.uuid4())
        entry = {"term": self.current_term, "key": key, "action": "cas", "old_value": old_value, "new_value": new_value, "guid": guid}
        self.log.append(entry)
        self.save_to_disk()
        self.acked_length[self.node_id] = len(self.log)

        for follower in self.cluster:
            if follower != self.node_id:
                self.replicate_log(self.node_id, follower)
        return True, guid
        
        


    def replicate_log(self, leader_id, follower_id):
        """Отправить AppendEntries RPC с новой записью журналов."""
        i = self.sent_length[follower_id]
        entries = self.log[i:len(self.log)]
        prev_log_term = 0
        if i > 0:
            prev_log_term = self.log[i - 1]["term"]

        payload = {
            "leader_id": leader_id,
            "term": self.current_term,
            "log_length": i,
            "prev_log_term": prev_log_term,
            "commit_length": self.commit_length,
            "entries": entries
        }
        self.send_post_http_request(f"http://node{follower_id}:{12121 + follower_id}/raft/log_request", payload) # node_id + 12121

    #############################
    # RPC Handlers
    #############################

    def log_request_handler(self, data):
        """Обработать LogRequest RPC от лидера."""
        leader_id = data["leader_id"]
        term = data["term"]
        log_length = data["log_length"]
        log_term = data["prev_log_term"]
        leader_commit = data["commit_length"]
        entries = data["entries"]

        # print(f"Received LogRequest {leader_id} {entries}")

        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.current_role = "follower"
            self.current_leader = leader_id
            self.last_heartbeat = time.time()
            self.save_to_disk()
        
        if term == self.current_term and self.current_role == "candidate":
            self.current_role = "follower"
            self.current_leader = leader_id
            self.voted_for = None
            self.last_heartbeat = time.time()
            self.save_to_disk()
        
        log_ok = (len(self.log) >= log_length) and (log_length == 0 or log_term == self.log[log_length - 1]["term"])
        if term == self.current_term and log_ok:
            self.current_leader = leader_id
            self.voted_for = None
            self.last_heartbeat = time.time()
            print("Append entries due to log_request_handler")
            self.append_entries(log_length, leader_commit, entries)
            ack = log_length + len(entries)
            payload = {
                "node_id": self.node_id,
                "current_term": self.current_term,
                "ack": ack,
                "status": True,
            }
            self.send_post_http_request(f"http://node{leader_id}:{12121 + leader_id}/raft/log_response", payload)
        else:
            payload = {
                "node_id": self.node_id,
                "current_term": self.current_term,
                "ack": 0,
                "status": False,
            }
            self.send_post_http_request(f"http://node{leader_id}:{12121 + leader_id}/raft/log_response", payload)
        

    def append_entries(self, log_length, leader_commit, entries):
        if len(entries) > 0 and len(self.log) > log_length:
            if self.log[log_length]["term"] != entries[0]["term"]:
                self.log = self.log[0:len(log_length)]
        if log_length + len(entries) > len(self.log):
            for i in range(len(self.log) - log_length, len(entries)):
                self.log.append(entries[i]) 
        if leader_commit > self.commit_length:
            for i in range(self.commit_length, leader_commit):
                self.deliver_msg_to_app(i)
            self.commit_length = leader_commit

        self.save_to_disk()


    def log_response_handler(self, data):
        follower = data["node_id"]
        term = data["current_term"]
        ack = data["ack"]
        success = data["status"]

        if term == self.current_term and self.current_role == "leader":
            if success == True and ack >= self.acked_length[follower]:
                self.sent_length[follower] = ack
                self.acked_length[follower] = ack
                self.commit_log_entries()
            elif self.sent_length[follower] > 0:
                self.sent_length[follower] = self.sent_length[follower] - 1
                self.replicate_log(self, self.node_id, follower)
        elif term > self.current_term:
            self.current_term = term
            self.current_role = "follower"
            self.voted_for = None

        self.save_to_disk()
    

    def get_acks_length(self, length) -> int:
        # для лидера нет acked_length
        number_of_nodes = 0
        for key in self.acked_length:
            if self.acked_length[key] >= length:
                number_of_nodes += 1
        return number_of_nodes
    

    def get_max_ready(self, min_acks) -> int:
        for i in range(len(self.log), 0, -1):
            if self.get_acks_length(i) >= min_acks:
                return i
        return 0
    

    def commit_log_entries(self):
        min_acks = math.ceil((len(self.cluster) + 1) / 2)
        max_ready = self.get_max_ready(min_acks)
        if max_ready != 0 and max_ready >= self.commit_length and self.log[max_ready - 1]["term"] == self.current_term:
            for i in range(self.commit_length, max_ready):
                self.deliver_msg_to_app(i)
            self.commit_length = max_ready
        
        self.save_to_disk()
    
    def deliver_msg_to_app(self, i):
        key = self.log[i]["key"]
        if self.log[i]["action"] == "create":
            value = self.log[i]["value"]
            self.data[key] = value
        if self.log[i]["action"] == "update":
            value = self.log[i]["value"]
            self.data[key] = value
        if self.log[i]["action"] == "delete":
            self.data.pop(key)
        if self.log[i]["action"] == "cas":
            guid = self.log[i]["guid"]
            old_value = self.log[i]["old_value"]
            new_value = self.log[i]["new_value"]
            success = False
            if self.data[key] == old_value:
                self.data[key] = new_value
                success = True
            self.data[guid] = success
        self.save_to_disk()
            
    
    def election_timer(self):
        """Check for election timeout and start elections if needed."""
        while True:
            if self.current_role == "follower" and (time.time() - self.last_heartbeat > self.election_timeout):
                self.last_heartbeat = time.time() + self.election_timeout # чтобы какое-то время не тревожил новыми выборами
                self.start_election()
            time.sleep(0.5)
    
    def start_election(self):
        """Start a new election."""
        self.current_term += 1
        self.current_role = "candidate"
        self.voted_for = self.node_id
        self.save_to_disk()

        self.votes_received = {self.node_id}
        last_term = 0

        if len(self.log) > 0:
            last_term = self.log[len(self.log) - 1]["term"]

        msg = {
            "node_id": self.node_id,
            "term": self.current_term,
            "log_length": len(self.log),
            "last_term": last_term
        }

        print(f"[Node {self.node_id}] Starting election for term {self.current_term}")

        for peer in self.cluster:
            if peer != self.node_id:
                self.send_post_http_request(f"http://node{peer}:{12121 + peer}/raft/request_vote", msg)
    
    
    def response_vote_handler(self, data):
        voter_id = data["node_id"]
        term = data["term"]
        granted = data["vote_granted"]

        if self.current_role == "candidate" and term == self.current_term and granted:
            self.votes_received.add(voter_id)
            if len(self.votes_received) >= math.ceil((len(self.cluster) + 1) / 2):
                self.become_leader()
        elif term > self.current_term:
            self.current_term = term
            self.current_role = "follower"
            self.voted_for = None
        self.save_to_disk()

    
    def become_leader(self):
        """Become the leader."""
        self.current_role = "leader"
        self.current_leader = self.node_id
        self.save_to_disk()
        print(f"[Node {self.node_id}] Became leader for term {self.current_term}")
        # first hearbeat
        for follower in self.cluster:
            if follower != self.node_id:
                self.sent_length[follower] = len(self.log)
                self.acked_length[follower] = 0
                self.replicate_log(self.node_id, follower)

    
    def send_heartbeat(self):
        """Leader sends heartbeats to all followers."""
        while True:
            if self.current_role == "leader":
                for follower in self.cluster:
                    if follower != self.node_id:
                        self.replicate_log(self.node_id, follower)
            time.sleep(self.heartbeat_interval)
    
    def request_vote_handler(self, data):
        """Handle RequestVote RPC."""
        c_id = data["node_id"]
        c_term = data["term"]
        c_log_length = data["log_length"]
        c_log_term = data["last_term"]

        my_log_term = self.log[len(self.log) - 1]["term"] if len(self.log) > 0 else 0

        log_ok = (c_log_term > my_log_term) or (c_log_term == my_log_term and c_log_length >= len(self.log))
        term_ok = (c_term > self.current_term) or (c_term == self.current_term and (self.voted_for is None or self.voted_for == c_id))

        if log_ok and term_ok:
            self.current_term = c_term
            self.current_role = "follower"
            self.voted_for = c_id
            self.save_to_disk() # TODO: проверить что флашится всегда когда надо

            msg = {
                "node_id": self.node_id,
                "term": self.current_term,
                "vote_granted": True,
            }
            
            self.send_post_http_request(f"http://node{c_id}:{12121 + c_id}/raft/response_vote", msg)
        else:
            msg = {
                "node_id": self.node_id,
                "term": self.current_term,
                "vote_granted": False,
            }
            self.send_post_http_request(f"http://node{c_id}:{12121 + c_id}/raft/response_vote", msg)
    
    
    def send_post_http_request(self, url, msg):
        try:
            requests.post(url, json=msg, timeout=0.0001).json()
        except requests.exceptions.RequestException as e:
            pass


node = None

@app.route('/raft/log_request', methods=['POST'])
def log_request():
    data = request.get_json()
    node.log_request_handler(data)
    return '', 204

@app.route('/raft/log_response', methods=['POST'])
def log_response():
    data = request.get_json()
    node.log_response_handler(data)
    return '', 204

@app.route('/raft/request_vote', methods=['POST'])
def request_vote():
    data = request.get_json()
    node.request_vote_handler(data)
    return '', 204

@app.route('/raft/response_vote', methods=['POST'])
def response_vote():
    data = request.get_json()
    node.response_vote_handler(data)
    return '', 204



@app.route('/kv', methods=['POST'])
def kv_create():
    key = request.json.get("key")
    value = request.json.get("value")
    status = node.handle_client_action(key, value, "create")
    if status != -1:
        return jsonify({"error": "Not leader", "leader_id": status}), 302
    while True:
        if key in node.data:
            return jsonify({"key": key, "value": node.data.get(key, None)}), 201
        time.sleep(0.1)
        
@app.route('/kv/<key>', methods=['GET'])
def kv_get(key):
    if key not in node.data:
        return 'Not found', 404
    return jsonify({"key": key, "value": node.data.get(key, None)}), 200

@app.route('/kv/<key>', methods=['PUT'])
def kv_update(key):
    if key not in node.data:
        return 'Not found', 404
    value = request.json.get("value")
    status = node.handle_client_action(key, value, "update")
    if status != -1:
        return jsonify({"error": "Not leader", "leader_id": status}), 302
    return '', 204

@app.route('/kv/<key>', methods=['DELETE'])
def kv_delete(key):
    if key not in node.data:
        return 'Not found', 404
    status = node.handle_client_action(key, node.data.get(key, None), "delete")
    if status != -1:
        return jsonify({"error": "Not leader", "leader_id": status}), 302
    return '', 204

@app.route('/kv/cas/<key>', methods=['PATCH'])
def cas(key):
    if key not in node.data:
        return 'Not found', 404
    old_value = request.json.get("old_value")
    new_value = request.json.get("new_value")
    status0, status1  = node.handle_cas(key, old_value, new_value)
    if not status0:
        return jsonify({"error": "Not leader", "leader_id": status1}), 302
    guid = status1
    while True:
        if guid in node.data:
            return jsonify({"status": node.data.get(guid)}), 200
        time.sleep(0.1)


if __name__ == "__main__":
    import sys
    node_id = int(sys.argv[1])
    cluster = [0, 1, 2, 3]
    node = RaftNode(node_id, cluster)
    app.run(host="0.0.0.0", port=12121 + node_id)