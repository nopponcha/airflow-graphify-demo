# Airflow + Graphify Demo บน Minikube (MacBook M2 / arm64)

เดโมนี้รัน **Apache Airflow 3.3.2 (LocalExecutor)** และ **Graphify** (เครื่องมือแปลง source code เป็น knowledge graph, https://github.com/Graphify-Labs/graphify) บน minikube  
Graphify วิเคราะห์ซอร์สโค้ดของ Airflow (`airflow-core/src/airflow`) แบบ **AST ล้วน ไม่ใช้ LLM และไม่ต้องมี API key**

**Task ใน DAG รันบน Kubernetes จริง** ผ่าน `KubernetesPodOperator` (provider `apache-airflow-providers-cncf-kubernetes`) — แต่ละ task สปอว์ pod แยก (`python:3.12-slim`, `busybox`, ฯลฯ) ใน namespace `airflow-demo` แล้วลบ pod เมื่อจบ (`on_finish_action=delete_pod`)

## โครงสร้าง
```
graphify/   Dockerfile + analyze.sh + entrypoint.sh  (clone airflow → graphify → เสิร์ฟผลที่พอร์ต 8080)
airflow/    Dockerfile (apache/airflow:3.3.2 + cncf-kubernetes provider) + dags/
            dags/demo_etl.py              ETL 3 ขั้น แต่ละขั้นเป็น pod
            dags/graphify_report.py       ดึงสถิติจาก service graphify ผ่าน pod
            dags/graphify_analyze_job.py  (optional) รัน analyze.sh ใน pod graphify-airflow:local
            dags/k8s_demo_common.py       ค่า default ของ KPO / RBAC
k8s/        namespace, RBAC (ServiceAccount airflow), secret/configmap, postgres, init job, airflow, graphify
scripts/    build.sh deploy.sh teardown.sh
Makefile    make start | build | deploy | ui | graph | watch-pods | validate | teardown
```

## สิ่งที่ต้องมี
- Docker Desktop (หรือ colima), `minikube`, `kubectl` (`brew install minikube kubectl`)
- Internet ระหว่าง build (pull image + clone GitHub)

## ขั้นตอน
```bash
# 1) เริ่ม minikube (arm64 อัตโนมัติบน M2)
minikube start --cpus 4 --memory 8g --driver=docker

# 2) build image เข้า docker daemon ของ minikube โดยตรง (ไม่ต้องใช้ registry)
eval $(minikube docker-env)
docker build -t graphify-airflow:local graphify/     # ~3-6 นาที (clone + วิเคราะห์ตอน build)
docker build -t airflow-demo:local airflow/        # ติดตั้ง cncf-kubernetes provider ตาม constraints 3.3.2
#   หรือ ./scripts/build.sh

# 3) deploy (รวม RBAC สำหรับ KPO)
./scripts/deploy.sh

# 4) เปิดหน้าเว็บ
minikube service -n airflow-demo airflow-api-server   # Airflow UI (NodePort 30080)  user/pass: admin/admin
minikube service -n airflow-demo graphify             # Graphify   (NodePort 30081)
# บน Mac ที่ใช้ docker driver ต้องเปิด terminal ค้างไว้ หรือใช้ port-forward:
kubectl -n airflow-demo port-forward svc/airflow-api-server 8080:8080
kubectl -n airflow-demo port-forward svc/graphify 8081:8080
```
> manifest ใช้ `imagePullPolicy: Never` กับ tag `:local` ดังนั้นต้อง build หลัง `eval $(minikube docker-env)` เสมอ  
> (ทางเลือก: build ปกติแล้ว `minikube image load graphify-airflow:local airflow-demo:local`)

## KubernetesPodOperator (KPO) และ RBAC
- **LocalExecutor** ยังใช้อยู่: scheduler (และ component อื่น) **สั่ง** ให้ Kubernetes สร้าง pod ต่อ task ไม่ได้รัน Python task ใน process ของ scheduler โดยตรง
- Scheduler / triggerer / dag-processor / api-server ใช้ **ServiceAccount `airflow`** (`k8s/05-airflow-rbac.yaml`)
- Role `airflow-pod-manager` อนุญาตใน namespace `airflow-demo`: จัดการ `pods`, อ่าน `pods/log`, `pods/exec`, ดู `events`
- DAG ตั้ง `in_cluster=True`, `namespace=airflow-demo`, `service_account_name=airflow`, `get_logs=True`, `on_finish_action=delete_pod`
- ส่งข้อมูลระหว่าง task: **XCom** ผ่าน `/airflow/xcom/return.json` (`do_xcom_push=True`) หรือ env ที่ render จาก `ti.xcom_pull(...)`

### ดู task pod ตอน demo
```bash
# terminal แยก — จะเห็น pod ชื่อ demo-etl-* / graphify-* โผล่แล้วหายเมื่อ task จบ
make watch-pods
# หรือ
kubectl get pods -n airflow-demo -w
```

## การใช้งาน
- Airflow UI → DAG `demo_simple_etl`, `graphify_code_report`, (optional) `graphify_analyze_job` → Trigger → ดู Logs + จอ `kubectl get pods -w`
- Graphify → `graph.html`, `GRAPH_REPORT.md`, `stats.json`
- วิเคราะห์ใหม่ตอนเริ่ม pod: `kubectl -n airflow-demo set env deploy/graphify ANALYZE_ON_START=true`
- วิเคราะห์แบบ one-off ผ่าน Airflow: trigger DAG `graphify_analyze_job` (pod ใช้ `graphify-airflow:local`, `image_pull_policy=Never`, ใช้เวลา ~1–3 นาที + ต้องมี egress ไป GitHub)

## Airflow 3 components ที่ deploy
| Deployment | คำสั่ง | หมายเหตุ |
|---|---|---|
| airflow-init (Job) | `airflow db migrate`, `airflow fab-db migrate`, `airflow users create` | สร้าง admin |
| airflow-api-server | `airflow api-server` | UI + REST + Execution API |
| airflow-scheduler | `airflow scheduler` | LocalExecutor + KPO สร้าง task pods |
| airflow-dag-processor | `airflow dag-processor` | โหลด DAG |
| airflow-triggerer | `airflow triggerer` | deferrable / async |

ค่า config สำคัญ: `AIRFLOW__CORE__EXECUTOR=LocalExecutor`, `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`, JWT secret ร่วมกันทุก component, `GRAPHIFY_URL` ใน ConfigMap

## Provider ใน image
`airflow/Dockerfile` ติดตั้ง `apache-airflow-providers-cncf-kubernetes==10.22.0` ด้วย  
`constraints-3.3.2/constraints-3.12.txt` จาก Apache Airflow

## หมายเหตุ Graphify
- ต้องรัน graphify จาก **โฟลเดอร์แม่** (`airflow-core/src`) ไม่ใช่ข้างใน `airflow/` เพราะ `airflow/logging/` บังโมดูล `logging` ของ Python
- ใช้ `graphify update --no-cluster` + `graphify cluster-only --no-label` ⇒ ไม่เรียก LLM

## ลบทิ้ง
```bash
./scripts/teardown.sh            # ลบ namespace
./scripts/teardown.sh --images   # ลบ image ด้วย
minikube stop
```

## Troubleshooting
- `ErrImageNeverPull` → ยังไม่ได้ build ใน docker ของ minikube
- task pod `Forbidden` / RBAC → ตรวจว่า apply `k8s/05-airflow-rbac.yaml` และ deployment มี `serviceAccountName: airflow`
- task pod `ImagePullBackOff` (python/busybox) → minikube ต้อง pull ได้จาก Docker Hub ครั้งแรก
- `graphify_analyze_job` ล้ม → ต้อง build `graphify-airflow:local` ใน minikube และ pod ต้องออก internet ได้
- รหัสผ่านใน `k8s/10-secrets-config.yaml` เป็นค่าเดโมเท่านั้น

## Validate ใน repo
```bash
make validate   # py_compile DAGs + kubeconform -strict (ถ้ามี kubeconform)
```
