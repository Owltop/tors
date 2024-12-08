import time
import random
import threading
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

class RaftNode:
    def __init__(self, node_id, cluster):
        self.node_id = node_id
        self.cluster = cluster
        self.state = "follower"  # "follower", "candidate", "leader"
        self.current_term = 0
        self.voted_for = None
        self.log = []  # [{term, key, value}]
        self.commit_index = -1
        self.last_applied = -1
        self.data = {}
        self.leader_id = None

        # Timing configs
        self.election_timeout = random.uniform(1.5, 3.0)
        self.last_heartbeat = time.time()
        self.heartbeat_interval = 0.5

        # Start background threads
        threading.Thread(target=self.election_timer, daemon=True).start()

    #############################
    # Raft Leader Responsibilities
    #############################

    def send_heartbeat(self):
        """Leader sends heartbeats to all followers."""
        while self.state == "leader":
            for peer in self.cluster:
                if peer != self.node_id:
                    threading.Thread(target=self.send_append_entries, args=(peer,), daemon=True).start()
            time.sleep(self.heartbeat_interval)

    def send_append_entries(self, peer):
        """Send AppendEntries RPC to a follower."""
        try:
            prev_log_index = len(self.log) - 1
            prev_log_term = self.log[prev_log_index]["term"] if prev_log_index >= 0 else 0

            payload = {
                "term": self.current_term,
                "leader_id": self.node_id,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": [],
                "leader_commit": self.commit_index
            }
            response = requests.post(f"http://{peer}/raft/append_entries", json=payload, timeout=1).json()

            # Update leader's term if needed
            if response["term"] > self.current_term:
                self.become_follower(response["term"])
        except requests.exceptions.RequestException:
            pass

    #############################
    # Election and Leader Election
    #############################

    def start_election(self):
        """Start a new election."""
        self.state = "candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        votes = 1
        print(f"[Node {self.node_id}] Starting election for term {self.current_term}")

        for peer in self.cluster:
            if peer != self.node_id:
                threading.Thread(target=self.request_vote, args=(peer, votes), daemon=True).start()

    def request_vote(self, peer, votes):
        """Send RequestVote RPC to a peer."""
        try:
            last_log_index = len(self.log) - 1
            last_log_term = self.log[last_log_index]["term"] if last_log_index >= 0 else 0

            payload = {
                "term": self.current_term,
                "candidate_id": self.node_id,
                "last_log_index": last_log_index,
                "last_log_term": last_log_term
            }
            response = requests.post(f"http://{peer}/raft/request_vote", json=payload, timeout=1).json()

            # Collect votes
            if response["vote_granted"]:
                votes += 1
                if votes > len(self.cluster) // 2 and self.state == "candidate":
                    self.become_leader()
        except requests.exceptions.RequestException:
            pass

    #############################
    # State Management
    #############################

    def become_leader(self):
        """Become the leader."""
        self.state = "leader"
        self.leader_id = self.node_id
        print(f"[Node {self.node_id}] Became leader for term {self.current_term}")
        threading.Thread(target=self.send_heartbeat, daemon=True).start()

    def become_follower(self, term):
        """Transition to follower state."""
        self.state = "follower"
        self.current_term = term
        self.voted_for = None
        self.last_heartbeat = time.time()

    def election_timer(self):
        """Check for election timeout and start elections if needed."""
        while True:
            if self.state == "follower" and (time.time() - self.last_heartbeat > self.election_timeout):
                self.start_election()
            time.sleep(0.5)

    #############################
    # RPC Handlers
    #############################

    def append_entries(self, data):
        """Handle AppendEntries RPC from the leader."""
        term = data["term"]
        if term < self.current_term:
            return {"term": self.current_term, "success": False}

        self.last_heartbeat = time.time()
        self.current_term = term
        self.leader_id = data["leader_id"]
        self.state = "follower"
        return {"term": self.current_term, "success": True}

    def request_vote_handler(self, data):
        """Handle RequestVote RPC."""
        term = data["term"]
        if term < self.current_term:
            return {"term": self.current_term, "vote_granted": False}

        if self.voted_for is None or self.voted_for == data["candidate_id"]:
            self.voted_for = data["candidate_id"]
            self.current_term = term
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

@app.route('/kv/<key>', methods=['GET', 'PUT'])
def data_operations(key):
    if request.method == 'GET':
        return jsonify({"key": key, "value": node.data.get(key, None)})

    if request.method == 'PUT':
        value = request.json.get("value")
        node.data[key] = value
        return jsonify({"key": key, "value": value})

if __name__ == "__main__":
    import sys
    node_id = sys.argv[1]
    cluster = sys.argv[2].split(",")
    node = RaftNode(node_id, cluster)
    app.run(host="0.0.0.0", port=int(node_id.split(":")[-1]))