# KubeQueue — Smart Queue Management Prototype

A deadline-friendly, runnable prototype aligned with the KubeQueue CNAD presentation. It demonstrates priority-aware dispatch, a lightweight arrivals forecast, simulated scaling decisions, queue monitoring, and a small benchmark comparison.

## Implemented
- Task submission into Critical / Normal / Batch queues
- Weighted dispatch opportunities (5 critical : 3 normal : 1 batch)
- Load-surge generator for demonstration
- Moving-average forecast of recent arrivals
- Scaling-policy comparison: KubeQueue, Static, HPA baseline, KEDA baseline
- Live dashboard with queue depth, processed tasks, throughput, replica count, events, and completion latency
- Dockerfile, Docker Compose, and a basic Kubernetes Deployment/Service manifest

## Monitoring and benchmark additions
- `GET /metrics` exposes Prometheus-format queue, priority, replica recommendation, submitted/processed, and forecast metrics.
- `GET /api/benchmark` returns transparent policy estimates for the current queue depth.
- Docker Compose starts the web app, Prometheus, and Grafana.
- Prometheus: `http://localhost:9090`; Grafana: `http://localhost:3000` (login `admin`, password `kubequeue123`). The provisioned dashboard is named **KubeQueue Metrics**.

## Important prototype boundary
This demo stores tasks in application memory. Replica counts and HPA/KEDA policies are simulations; it does not create real consumer pods or connect to RabbitMQ/Kafka, Prometheus, Grafana, or a Kubernetes API. Do not report simulated values as production benchmarks. These are the next integration steps if more time is available.

## Run on Windows 11
1. Install Python 3.10+ if needed.
2. Extract this folder.
3. Open Command Prompt or VS Code terminal in the `KubeQueue` folder.
4. Run:
   ```bat
   py -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   python app.py
   ```
5. Open `http://127.0.0.1:5000`.

If `py` is not recognized, try `python` instead. Keep the terminal open while using the dashboard.

## Demo script (2–3 minutes)
1. Open dashboard and show zero queue depth.
2. Add one critical task and one batch task.
3. Click **Generate 30-task surge**. Point to the queue-depth metric, forecast, and replica recommendation.
4. Select **Static**, **HPA baseline**, and **KEDA baseline** to explain the different simulated policies.
5. Select **KubeQueue — queue + forecast** again.
6. Click **Process next consumer batch** and show completed tasks and latency.
7. Switch back to KubeQueue, process until queue depth falls, and show scale-down.
8. Explain that this prototype simulates controller decisions; real Kubernetes pod scaling is a future integration.

## Docker run
With Docker Desktop running:
```bash
docker compose up --build
```
Then open `http://localhost:5000`. Stop with `Ctrl+C`; optional cleanup: `docker compose down`.

## Kubernetes (optional, only after Docker build)
Build the image:
```bash
docker build -t kubequeue:local .
```
For Minikube, load the local image:
```bash
minikube image load kubequeue:local
kubectl apply -f k8s/deployment.yaml
kubectl get pods
kubectl port-forward service/kubequeue 5000:5000
```
Open `http://127.0.0.1:5000`. This manifest deploys the web app only; it does not scale consumer pods based on the simulated replica count.

## API endpoints
- `GET /api/status` — current metrics and queues
- `POST /api/tasks` — add one task (`{"name":"Example","priority":"critical"}`)
- `POST /api/surge` — generate tasks (`{"count":30}`)
- `POST /api/process` — process a batch (`{"count":10}`)
- `POST /api/strategy` — select strategy (`{"strategy":"kubequeue"}`)
- `POST /api/reset` — reset demo
- `GET /health` — health check


## Monitoring stack run
From the project folder, with Docker Desktop running:
```powershell
docker compose up --build
```
Open the app at `http://localhost:5000`, Prometheus at `http://localhost:9090`, and Grafana at `http://localhost:3000`. Grafana credentials for this local demo are `admin` / `kubequeue123`; change them before any non-local deployment.
