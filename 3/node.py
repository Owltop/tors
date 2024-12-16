import time
from flask import Flask, request, jsonify
import threading
import requests
import json

app = Flask(__name__)

state = {}  
vector_clock = {} 
known_replicas = set()


def merge_states(remote_state, remote_vector_clock):
    """Синхронизация локального состояния с удаленным."""

    for key, remote_entry in remote_state.items():
        local_entry = state.get(key)
        remote_vclock = remote_vector_clock.get(key, {})

        if (
            local_entry is None
            or compare_vector_clocks(remote_vclock, vector_clock.get(key, {})) > 0
        ):
            state[key] = remote_entry
            vector_clock[key] = remote_vclock


def compare_vector_clocks(vc1, vc2):
    keys = set(vc1.keys()).union(vc2.keys())
    vc1_newer, vc2_newer = False, False

    for key in keys:
        counter1 = vc1.get(key, 0)
        counter2 = vc2.get(key, 0)
        if counter1 > counter2:
            vc1_newer = True
        elif counter1 < counter2:
            vc2_newer = True

    if vc1_newer and not vc2_newer:
        return 1
    elif vc2_newer and not vc1_newer:
        return -1
    else:
        return 0


def broadcast_to_replicas(endpoint, data):
    for replica in known_replicas:
        try:
            requests.post(f"http://{replica}{endpoint}", json=data, timeout=1)
        except requests.exceptions.RequestException:
            pass


def start_replica_sync():
    # Если ничего не происходит, реплики всё равно синхронизируются
    def sync_loop():
        while True:
            for replica in known_replicas:
                try:
                    response = requests.get(f"http://{replica}/state", timeout=1)
                    if response.status_code == 200:
                        remote_data = response.json()
                        merge_states(remote_data["state"], remote_data["vector_clock"])
                except requests.exceptions.RequestException:
                    pass
            time.sleep(5)

    t = threading.Thread(target=sync_loop, daemon=True)
    t.start()



@app.route("/patch", methods=["PATCH"])
def patch():
    data = request.json
    changes = data.get("changes", {})

    replica_id = request.host
    for key, value in changes.items():
        vector_clock[key] = vector_clock.get(key, {})
        vector_clock[key][replica_id] = vector_clock[key].get(replica_id, 0) + 1
        state[key] = value

    broadcast_to_replicas("/sync", {"state": state, "vector_clock": vector_clock})

    return jsonify({"status": "ok"})


@app.route("/sync", methods=["POST"])
def sync():
    data = request.json
    if not data or "state" not in data or "vector_clock" not in data:
        return jsonify({"status": "error", "message": "Invalid request"}), 400

    merge_states(data["state"], data["vector_clock"])
    return jsonify({"status": "ok"})


@app.route("/state", methods=["GET"])
def get_state():
    """Возвращает текущее состояние реплики."""
    return jsonify({"state": state, "vector_clock": vector_clock})


@app.route("/replicas", methods=["POST"])
def add_replica():
    """Добавляет новую известную реплику."""
    data = request.json
    new_replica = data.get("replica")
    if not new_replica:
        return jsonify({"status": "error", "message": "Invalid request"}), 400

    known_replicas.add(new_replica)
    return jsonify({"status": "ok"})


@app.route("/state/<key>", methods=["GET"])
def get_key_state(key):
    """Возвращает значение ключа."""
    if key in state:
        return jsonify({"key": key, "value": state[key]['value'], "vector_clock": vector_clock[key]})
    return jsonify({"status": "error", "message": "Key not found"}), 404


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="CRDT Node")
    parser.add_argument("--port", type=int, default=5000, help="Port to run the server on")
    parser.add_argument("--replicas", nargs="*", default=[], help="Initial list of known replicas")
    args = parser.parse_args()

    known_replicas.update(args.replicas)

    start_replica_sync()


    app.run(host="0.0.0.0", port=args.port)
