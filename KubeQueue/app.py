from flask import Flask, jsonify, render_template, request
from collections import deque
from datetime import datetime
import threading
import time
import uuid

app = Flask(__name__)

# In-memory queues keep this deadline-friendly prototype easy to run.
# Production version: replace these with RabbitMQ/Kafka and persistent metrics.
LOCK = threading.RLock()
QUEUES = {"critical": deque(), "normal": deque(), "batch": deque()}
HISTORY = []
COMPLETED = []
EVENTS = []
REPLICA_COUNT = 1
MIN_REPLICAS, MAX_REPLICAS = 1, 10
STRATEGY = "kubequeue"
ARRIVAL_WINDOW = deque(maxlen=12)
STARTED = time.time()
COUNTERS = {"submitted": 0, "processed": 0}
WEIGHTS = {"critical": 5, "normal": 3, "batch": 1}
WEIGHT_CYCLE = ["critical"] * 5 + ["normal"] * 3 + ["batch"]
cycle_index = 0

def now():
    return datetime.now().strftime("%H:%M:%S")

def queue_depth():
    return sum(len(q) for q in QUEUES.values())

def snapshot():
    depth = queue_depth()
    throughput = len(COMPLETED) / max((time.time() - STARTED) / 60, 1/60)
    return {
        "time": now(), "queue_depth": depth,
        "critical_depth": len(QUEUES["critical"]),
        "normal_depth": len(QUEUES["normal"]),
        "batch_depth": len(QUEUES["batch"]),
        "replicas": REPLICA_COUNT, "throughput_per_min": round(throughput, 2),
        "submitted": COUNTERS["submitted"], "processed": COUNTERS["processed"]
    }

def forecast():
    values = list(ARRIVAL_WINDOW)
    if not values:
        return 0
    # Lightweight moving-average predictor based on recent task arrivals.
    recent = values[-5:]
    return round(sum(recent) / len(recent), 2)

def decide_scale():
    global REPLICA_COUNT
    depth = queue_depth()
    predicted = forecast()
    if STRATEGY == "static":
        desired = 2
        reason = "Static baseline: fixed at 2 replicas"
    elif STRATEGY == "hpa":
        # CPU is not measured in this prototype; use a documented proxy baseline.
        desired = max(1, min(MAX_REPLICAS, 1 + depth // 12))
        reason = "HPA baseline approximation: reactive load proxy"
    elif STRATEGY == "keda":
        desired = max(1, min(MAX_REPLICAS, 1 + depth // 6))
        reason = "KEDA baseline approximation: reacts to current queue depth"
    else:
        desired = max(1, min(MAX_REPLICAS, 1 + int((depth + predicted * 2) // 5)))
        reason = f"KubeQueue: current backlog {depth} + forecast arrivals {predicted}"
    old = REPLICA_COUNT
    REPLICA_COUNT = desired
    if old != desired:
        EVENTS.insert(0, {"time": now(), "message": f"Scaling decision: {old} → {desired} replicas", "type": "scale"})
        del EVENTS[30:]
    return {"previous": old, "replicas": desired, "reason": reason, "forecast_arrivals": predicted}

def choose_next():
    global cycle_index
    # Weighted dispatch: 5 critical : 3 normal : 1 batch opportunities.
    for _ in range(len(WEIGHT_CYCLE)):
        priority = WEIGHT_CYCLE[cycle_index % len(WEIGHT_CYCLE)]
        cycle_index += 1
        if QUEUES[priority]:
            return QUEUES[priority].popleft()
    for priority in ("critical", "normal", "batch"):
        if QUEUES[priority]:
            return QUEUES[priority].popleft()
    return None

@app.get("/")
def index():
    return render_template("index.html")

@app.get("/api/status")
def status():
    with LOCK:
        data = snapshot()
        data.update({
            "forecast_arrivals": forecast(),
            "strategy": STRATEGY,
            "queues": {k: list(v) for k, v in QUEUES.items()},
            "recent_completed": COMPLETED[:12],
            "events": EVENTS[:10],
            "uptime_seconds": int(time.time() - STARTED)
        })
        return jsonify(data)

@app.post("/api/tasks")
def submit_task():
    body = request.get_json(silent=True) or {}
    priority = str(body.get("priority", "normal")).lower()
    if priority not in QUEUES:
        return jsonify({"error": "priority must be critical, normal, or batch"}), 400
    task = {
        "id": uuid.uuid4().hex[:8].upper(),
        "name": str(body.get("name", "Demo task"))[:80],
        "priority": priority,
        "created_at": now(),
        "created_ts": time.time()
    }
    with LOCK:
        QUEUES[priority].append(task)
        COUNTERS["submitted"] += 1
        ARRIVAL_WINDOW.append(1)
        EVENTS.insert(0, {"time": now(), "message": f"Task {task['id']} added to {priority} queue", "type": "task"})
        del EVENTS[30:]
        scale = decide_scale()
        return jsonify({"task": task, "scaling": scale, "status": snapshot()}), 201

@app.post("/api/surge")
def surge():
    body = request.get_json(silent=True) or {}
    count = max(1, min(100, int(body.get("count", 20))))
    with LOCK:
        for i in range(count):
            priority = "critical" if i % 5 == 0 else ("normal" if i % 3 else "batch")
            task = {"id": uuid.uuid4().hex[:8].upper(), "name": f"Surge task {i+1}", "priority": priority,
                    "created_at": now(), "created_ts": time.time()}
            QUEUES[priority].append(task)
            COUNTERS["submitted"] += 1
        ARRIVAL_WINDOW.append(count)
        EVENTS.insert(0, {"time": now(), "message": f"Load surge generated: {count} tasks", "type": "surge"})
        del EVENTS[30:]
        scale = decide_scale()
        return jsonify({"added": count, "scaling": scale, "status": snapshot()})

@app.post("/api/process")
def process_tasks():
    body = request.get_json(silent=True) or {}
    count = max(1, min(100, int(body.get("count", max(1, REPLICA_COUNT * 3)))))
    with LOCK:
        processed_now = []
        for _ in range(count):
            task = choose_next()
            if not task:
                break
            task["processed_at"] = now()
            task["latency_seconds"] = round(max(0, time.time() - task["created_ts"]), 2)
            COMPLETED.insert(0, task)
            del COMPLETED[100:]
            COUNTERS["processed"] += 1
            processed_now.append(task)
        if processed_now:
            EVENTS.insert(0, {"time": now(), "message": f"Processed {len(processed_now)} tasks using weighted priority dispatch", "type": "process"})
            del EVENTS[30:]
        scale = decide_scale()
        return jsonify({"processed": processed_now, "count": len(processed_now), "scaling": scale, "status": snapshot()})

@app.post("/api/strategy")
def set_strategy():
    global STRATEGY
    body = request.get_json(silent=True) or {}
    strategy = str(body.get("strategy", "kubequeue")).lower()
    if strategy not in ("kubequeue", "static", "hpa", "keda"):
        return jsonify({"error": "unknown strategy"}), 400
    with LOCK:
        STRATEGY = strategy
        result = decide_scale()
        EVENTS.insert(0, {"time": now(), "message": f"Strategy changed to {strategy.upper()}", "type": "strategy"})
        del EVENTS[30:]
        return jsonify({"strategy": STRATEGY, "scaling": result})

@app.post("/api/reset")
def reset():
    global REPLICA_COUNT, STRATEGY, STARTED, cycle_index
    with LOCK:
        for q in QUEUES.values():
            q.clear()
        HISTORY.clear()
        COMPLETED.clear()
        EVENTS.clear()
        ARRIVAL_WINDOW.clear()
        COUNTERS["submitted"] = COUNTERS["processed"] = 0
        REPLICA_COUNT, STRATEGY, cycle_index = 1, "kubequeue", 0
        STARTED = time.time()
        EVENTS.insert(0, {"time": now(), "message": "Demo reset", "type": "system"})
        return jsonify({"ok": True})


@app.get("/metrics")
def prometheus_metrics():
    """Prometheus text exposition endpoint for the prototype metrics."""
    with LOCK:
        lines = [
            '# HELP kubequeue_queue_depth Number of tasks waiting in all priority queues',
            '# TYPE kubequeue_queue_depth gauge',
            f'kubequeue_queue_depth {queue_depth()}',
            '# HELP kubequeue_priority_queue_depth Tasks waiting by priority',
            '# TYPE kubequeue_priority_queue_depth gauge',
        ]
        for priority, q in QUEUES.items():
            lines.append(f'kubequeue_priority_queue_depth{{priority="{priority}"}} {len(q)}')
        lines.extend([
            '# HELP kubequeue_consumer_replicas Current recommended consumer replicas (simulated)',
            '# TYPE kubequeue_consumer_replicas gauge',
            f'kubequeue_consumer_replicas {REPLICA_COUNT}',
            '# HELP kubequeue_tasks_submitted Total tasks submitted since app start/reset',
            '# TYPE kubequeue_tasks_submitted counter',
            f'kubequeue_tasks_submitted {COUNTERS["submitted"]}',
            '# HELP kubequeue_tasks_processed Total tasks processed since app start/reset',
            '# TYPE kubequeue_tasks_processed counter',
            f'kubequeue_tasks_processed {COUNTERS["processed"]}',
            '# HELP kubequeue_forecast_arrivals Moving-average forecast of incoming tasks',
            '# TYPE kubequeue_forecast_arrivals gauge',
            f'kubequeue_forecast_arrivals {forecast()}',
        ])
        return app.response_class('\n'.join(lines) + '\n', mimetype='text/plain; version=0.0.4; charset=utf-8')

@app.get("/api/benchmark")
def benchmark():
    """Run a deterministic policy comparison on the current backlog and forecast."""
    with LOCK:
        depth = queue_depth()
        predicted = forecast()
        scenarios = []
        for name, replicas in [
            ('Static', 2),
            ('HPA baseline', max(1, min(MAX_REPLICAS, 1 + depth // 12))),
            ('KEDA baseline', max(1, min(MAX_REPLICAS, 1 + depth // 6))),
            ('KubeQueue', max(1, min(MAX_REPLICAS, 1 + int((depth + predicted * 2) // 5)))),
        ]:
            # Transparent analytical estimates for comparison, not measured production results.
            capacity = replicas * 3
            estimated_drain_seconds = round(depth / max(capacity, 1), 2)
            scenarios.append({
                'strategy': name, 'replicas': replicas, 'estimated_capacity_per_batch': capacity,
                'estimated_drain_seconds': estimated_drain_seconds,
                'relative_resource_units': replicas,
                'note': 'Estimate from prototype policy; not a live cluster benchmark'
            })
        return jsonify({'queue_depth': depth, 'forecast_arrivals': predicted, 'results': scenarios,
                        'disclaimer': 'These are analytical estimates based on the current demo backlog, not measured latency or cloud cost.'})

@app.get("/health")
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
