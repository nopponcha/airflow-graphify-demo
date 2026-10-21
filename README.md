# Airflow + Graphify Demo บน Minikube (MacBook M2 / arm64)

เดโมนี้รัน **Apache Airflow 3 (LocalExecutor)** และ **Graphify** (เครื่องมือแปลง source code เป็น knowledge graph, https://github.com/Graphify-Labs/graphify) บน minikube
Graphify วิเคราะห์ซอร์สโค้ดของ Airflow (`airflow-core/src/airflow`) แบบ **AST ล้วน ไม่ใช้ LLM และไม่ต้องมี API key**

## โครงสร้าง
```
graphify/   Dockerfile + analyze.sh + entrypoint.sh  (clone airflow → graphify → เสิร์ฟผลที่พอร์ต 8080)
airflow/    Dockerfile (apache/airflow:3.3.2-python3.12) + dags/
            dags/demo_etl.py        ETL ตัวอย่าง extract → transform → load
            dags/graphify_report.py ดึงสถิติจาก service graphify มาสรุป (nodes/edges/god nodes/import cycles)
k8s/        namespace, secret/configmap, postgres (StatefulSet+PVC), init job, airflow, graphify
scripts/    build.sh deploy.sh teardown.sh
Makefile    make start | build | deploy | ui | graph | teardown
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
docker build -t airflow-demo:local airflow/
#   หรือ ./scripts/build.sh

# 3) deploy
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

## การใช้งาน
- Airflow UI → DAG `demo_simple_etl` และ `graphify_code_report` (ไม่ pause ตั้งแต่สร้าง) → กด Trigger → ดู Logs ของ task `report`
- Graphify → `graph.html` (กราฟ interactive), `GRAPH_REPORT.md`, `stats.json`
- วิเคราะห์ใหม่ตอนเริ่ม pod: `kubectl -n airflow-demo set env deploy/graphify ANALYZE_ON_START=true`
- build แบบไม่วิเคราะห์ตอน build: `docker build --build-arg ANALYZE_AT_BUILD=false ...` (จะวิเคราะห์ตอน pod start แทน)
- ล็อกเวอร์ชัน airflow ที่วิเคราะห์: `--build-arg AIRFLOW_REF=3.3.2`

## Airflow 3 components ที่ deploy
| Deployment | คำสั่ง | หมายเหตุ |
|---|---|---|
| airflow-init (Job) | `airflow db migrate`, `airflow fab-db migrate`, `airflow users create` | สร้าง admin |
| airflow-api-server | `airflow api-server` | UI + REST + Execution API (แทน webserver ใน Airflow 2) |
| airflow-scheduler | `airflow scheduler` | LocalExecutor รัน task ใน pod นี้ |
| airflow-dag-processor | `airflow dag-processor` | ใน Airflow 3 แยกออกจาก scheduler |
| airflow-triggerer | `airflow triggerer` | สำหรับ deferrable tasks |

ค่า config สำคัญ (ConfigMap/Secret): `AIRFLOW__CORE__EXECUTOR=LocalExecutor`, `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`,
`AIRFLOW__API_AUTH__JWT_SECRET` (ต้องเหมือนกันทุก component), `AIRFLOW__CORE__AUTH_MANAGER=FabAuthManager`

## หมายเหตุ Graphify
- ต้องรัน graphify จาก **โฟลเดอร์แม่** (`airflow-core/src`) ไม่ใช่ข้างใน `airflow/` เพราะ `airflow/logging/` บังโมดูล `logging` ของ Python
- ใช้ `graphify update --no-cluster` + `graphify cluster-only --no-label` ⇒ ไม่เรียก LLM; ชื่อ cluster จะเป็น "Community N"
- กราฟมี >5000 nodes ดังนั้น `graph.html` แสดงแบบรวมกลุ่ม (community view)

## ลบทิ้ง
```bash
./scripts/teardown.sh            # ลบ namespace
./scripts/teardown.sh --images   # ลบ image ด้วย
minikube stop
```

## Troubleshooting
- `ErrImageNeverPull` → ยังไม่ได้ build ใน docker ของ minikube (`eval $(minikube docker-env)` แล้ว build ใหม่)
- pod OOMKilled → `minikube start --memory 10g` หรือลด limit ใน k8s/
- task fail เรื่อง execution API → ตรวจ `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` และ JWT secret
- รหัสผ่านใน `k8s/10-secrets-config.yaml` เป็นค่าเดโมเท่านั้น
