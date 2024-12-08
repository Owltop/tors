import os
import time
import random
import json
import threading
import requests
from flask import Flask, request, jsonify

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
        self.election_timeout = random.uniform(1.5, 3.0)
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

    def handle_client_put(self, key, value):
        """Обработка PUT-запроса клиента на лидере."""
        if self.current_role != "leader":
            # Лидер отвечает клиенту: перенаправить запрос на лидера
            return jsonify({"error": "Not leader", "leader_id": self.current_leader}), 302

        # Добавить запись в журнал
        entry = {"term": self.current_term, "key": key, "value": value}
        self.log.append(entry)
        self.save_to_disk()

        # Реплицировать запись на всех фолловерах
        success_count = 1  # Лидер реплицировал локально
        for peer in self.cluster:
            if peer != self.node_id:
                if self.replicate_log(peer):
                    success_count += 1

        # Подтвердить запись, если большинство узлов успешно реплицировали
        if success_count > len(self.cluster) // 2:
            self.commit_index += 1
            self.apply_log_to_state()
            return jsonify({"key": key, "value": value})
        else:
            return jsonify({"error": "Failed to replicate on majority"}), 500

    def replicate_log(self, peer): # (TODO) do better
        """Отправить AppendEntries RPC с новой записью журналов."""
        try:
            prev_log_index = len(self.log) - 2
            prev_log_term = self.log[prev_log_index]["term"] if prev_log_index >= 0 else 0

            payload = {
                "term": self.current_term,
                "leader_id": self.node_id,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": [self.log[-1]],  # Отправляем только последнюю запись
                "leader_commit": self.commit_index
            }
            response = requests.post(f"http://{peer}/raft/append_entries", json=payload, timeout=1).json()
            return response.get("success", False)
        except requests.exceptions.RequestException:
            return False

    #############################
    # Применение логов к состоянию
    #############################

    def apply_log_to_state(self):
        """Применить подтвержденные записи из журнала в состояние (data)."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            self.data[entry["key"]] = entry["value"]
            self.save_to_disk()  # Сохраняем данные после применения

    #############################
    # RPC Handlers
    #############################

    def append_entries(self, data):
        """Обработать AppendEntries RPC от лидера."""
        term = data["term"]
        if term < self.current_term:
            return {"term": self.current_term, "success": False}

        self.current_term = term
        self.current_leader = data["leader_id"]
        self.last_heartbeat = time.time()
        self.current_role = "follower"

        # Проверить согласованность журнала
        prev_log_index = data["prev_log_index"]
        prev_log_term = data["prev_log_term"]
        if prev_log_index >= 0 and (len(self.log) <= prev_log_index or self.log[prev_log_index]["term"] != prev_log_term):
            return {"term": self.current_term, "success": False}

        # Добавить новые записи в журнал
        self.log = self.log[:prev_log_index + 1] + data["entries"]
        self.save_to_disk()

        # Обновить commitIndex, если получено от лидера
        if data["leader_commit"] > self.commit_index:
            self.commit_index = min(data["leader_commit"], len(self.log) - 1)
            self.apply_log_to_state()

        return {"term": self.current_term, "success": True}
    
    def election_timer(self):
        """Check for election timeout and start elections if needed."""
        while True:
            if self.current_role == "follower" and (time.time() - self.last_heartbeat > self.election_timeout):
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
                threading.Thread(target=self.request_vote, args=(peer, msg), daemon=True).start()
    
    def request_vote(self, peer, msg):
        """Send RequestVote RPC to a peer."""
        try:
            response = requests.post(f"http://{peer}/raft/request_vote", json=msg, timeout=1).json()
            # Collect votes -- may be long 
            if self.current_role == "candidate" and response["term"] == self.current_term and response["vote_granted"]:
                self.votes_received.add(peer)
                if len(self.votes_received) > len(self.cluster) // 2 and self.current_role == "candidate":
                    self.become_leader()
            elif response["term"] == self.current_term:
                self.current_term - response["term"]
                self.current_role = "follower"
                self.voted_for = None
            self.save_to_disk()
        except requests.exceptions.RequestException:
            pass
    
    def become_leader(self):
        """Become the leader."""
        self.current_role = "leader"
        self.current_leader = self.node_id
        print(f"[Node {self.node_id}] Became leader for term {self.current_term}")
        threading.Thread(target=self.send_heartbeat, daemon=True).start()
    
    def send_heartbeat(self):
        """Leader sends heartbeats to all followers."""
        while self.current_role == "leader":
            for peer in self.cluster:
                if peer != self.node_id:
                    threading.Thread(target=self.replicate_log, args=(peer,), daemon=True).start()
            time.sleep(self.heartbeat_interval)
    
    def request_vote_handler(self, data):
        """Handle RequestVote RPC."""
        c_id = data["node_id"]
        c_term = data["term"]
        c_log_term = data["last_term"]
        c_log_length = data["log_length"]

        my_log_term = self.log[len(self.log) - 1]["term"] if len(self.log) > 0 else 0

        log_ok = (c_log_term > my_log_term) or (c_log_term == my_log_term and c_log_length >= len(self.log))
        term_ok = (c_term > self.current_term) or (c_term == self.current_term and (self.voted_for is None or self.voted_for == c_id))

        if log_ok and term_ok:
            self.current_term = c_term
            self.current_role = "follower"
            self.voted_for = c_id
            self.save_to_disk() # TODO: проверить что флашится всегда когда надо
            return {"term": self.current_term, "vote_granted": True}

        return {"term": self.current_term, "vote_granted": False}

node = None

@app.route('/raft/append_entries', methods=['POST'])
def append_entries():
    data = request.get_json()
    response = node.append_entries(data)
    return jsonify(response)

@app.route('/raft/request_vote', methods=['POST'])
def request_vote():
    data = request.get_json()
    response = node.request_vote_handler(data)
    return jsonify(response)

@app.route('/kv/<key>', methods=['GET'])
def data_operations(key):
    return jsonify({"key": key, "value": node.data.get(key, None)}) # TODO think
    

@app.route('/kv/<key>', methods=['PUT'])
def client_put(key):
    value = request.json.get("value")
    response = node.handle_client_put(key, value)
    return response

if __name__ == "__main__":
    import sys
    node_id = sys.argv[1]
    cluster = sys.argv[2].split(",")
    node = RaftNode(node_id, cluster)
    app.run(host="0.0.0.0", port=int(node_id.split(":")[-1]))