# สคริปต์นำเสนอผู้บริหาร: "เข้าใจระบบใหญ่ใน 10 นาที ด้วย Graphify + Airflow" (~12-15 นาที)

> เตรียมก่อน: รัน `make start build deploy` ล่วงหน้า, เปิด port-forward 8080 (Airflow) และ 8081 (Graphify), login admin/admin, trigger DAG ไว้ 1 รอบ

## 0. เปิดเรื่อง (1 นาที)
**พูด:** "ซอฟต์แวร์องค์กรโตขึ้นทุกปี คนใหม่ใช้เวลาหลายสัปดาห์กว่าจะเข้าใจโค้ด วันนี้จะโชว์ว่าเราใช้เครื่องมือ open-source ทำ 'แผนที่' ของระบบขนาดใหญ่ได้อัตโนมัติ และรันบน Kubernetes แบบเดียวกับ production"
**จอ:** สไลด์ชื่อเรื่อง / README

## 1. โจทย์และตัวอย่าง: Apache Airflow (1.5 นาที)
**พูด:** "Airflow คือ workflow orchestrator ที่องค์กรทั่วโลกใช้ เป็นโปรเจกต์ open-source ขนาดใหญ่ ผู้ร่วมพัฒนาหลายพันคน — เหมาะเป็นตัวแทนระบบ legacy ขนาดใหญ่ขององค์กร"
**จอ:** github.com/apache/airflow

## 2. สถาปัตยกรรมเดโม (1.5 นาที)
**จอ:** `kubectl -n airflow-demo get pods,svc`
**พูด:** "ทุกอย่างอยู่บน laptop ผ่าน minikube: Postgres, Airflow 3 (api-server, scheduler, dag-processor, triggerer) และ Graphify — **แต่ละ task ใน DAG ไม่รันใน scheduler โดยตรง แต่สปอว์ Kubernetes pod จริงผ่าน KubernetesPodOperator** เหมือน production ที่ worker เป็น pod"
**จอ (terminal คู่):** `kubectl get pods -n airflow-demo -w` หรือ `make watch-pods`
**พูด:** "ServiceAccount `airflow` + Role ใน namespace ให้ scheduler สร้าง/ลบ pod task ได้ — ย้าย manifest ชุดนี้ขึ้น EKS/GKE ได้"
**คุณค่า:** ทดลองฟรี, ทำซ้ำได้, ไม่มีค่า cloud ระหว่าง PoC

## 3. Graphify คืออะไร (1.5 นาที)
**พูด:** "Graphify อ่านโค้ดด้วย parser (tree-sitter) แล้วสร้าง knowledge graph ของ class/function/import ทุกเส้นมีที่มา — **ไม่ใช้ AI ภายนอก ไม่ต้องมี API key โค้ดไม่ออกนอกเครื่อง** ⇒ ปลอดภัยต่อข้อมูลองค์กร ไม่มีค่า token"
**จอ:** หน้า index ของ Graphify (localhost:8081)

## 4. ตัวเลขภาพรวม (2 นาที)
**จอ:** `GRAPH_REPORT.md` ส่วน Summary / `stats.json`
**พูด (ผลจริงจากการวิเคราะห์ airflow-core):**
- **20,497 nodes** (class, function, module, ไฟล์ UI)
- **53,340 edges** (ความสัมพันธ์: เรียกใช้, import, สืบทอด) — 90% อ่านตรงจากโค้ด, 10% อนุมาน (ความมั่นใจเฉลี่ย 0.91)
- **630 clusters** (กลุ่มโมดูลที่เกาะกลุ่มกัน)
- ใช้เวลาวิเคราะห์ประมาณ 30 วินาที, ค่าใช้จ่าย token = 0
"ถ้าคนอ่านเองต้องใช้หลายสัปดาห์"

## 5. กราฟ interactive (2 นาที)
**จอ:** `graph.html` — ซูม, ค้นหา, คลิก cluster
**พูด:** "แต่ละวงคือกลุ่มโมดูล เส้นคือการพึ่งพากัน เห็นทันทีว่าส่วนไหนเป็นศูนย์กลาง ส่วนไหนแยกตัว เช่น cluster ฐานข้อมูล/migration, cluster API client ของ UI, cluster จัดการสถานะ task"

## 6. Insight สำคัญ (2.5 นาที)
**จอ:** `GRAPH_REPORT.md` ส่วน God Nodes และ Import Cycles
**พูด:**
- **หัวใจของระบบ (core abstractions):** `TaskInstance` (347 เส้น), `DagRun` (213), `SerializedDAG` (167), `SchedulerJobRunner` (150), รวมถึง `provide_session()` และ `ProvidersManager`
  → "ถ้าจะแก้ส่วนนี้ต้องระวังที่สุด เพราะกระทบทั้งระบบ = จุดที่ควรมีเทสต์และ reviewer อาวุโส"
- **Circular imports ใน UI:** พบ ~14 วงจร ส่วนใหญ่วนผ่าน `ui/src/utils/links.ts` ↔ `ui/src/router.tsx`
  → "เป็น technical debt ที่ชี้เป้าได้ทันที ลดความเสี่ยงบั๊กและทำให้ build/refactor ง่ายขึ้น"
- ฝั่ง UI ใช้ i18n (`react-i18next`) เชื่อมโยงมากที่สุด → รองรับหลายภาษาอย่างเป็นระบบ

## 7. ทำให้เป็นอัตโนมัติด้วย Airflow + KPO (2.5 นาที)
**จอ:** Airflow UI → Trigger DAG `demo_simple_etl` — ดู Graph view (extract → transform → load)
**จอคู่:** `kubectl get pods -n airflow-demo -w` — ชี้ให้เห็น pod `demo-etl-extract-*`, `demo-etl-transform-*`, `demo-etl-load-*` โผล่แล้วหาย (delete pod หลังจบ)
**พูด:** "แต่ละขั้น ETL คือ container image แยก (python / busybox) multi-arch บน M2 — ข้อมูลส่งต่อด้วย XCom จาก `/airflow/xcom/return.json`"
**จอ:** Trigger `graphify_code_report` → Logs ของ task `report` (pod ยิง HTTP ไป Service `graphify` ใน cluster)
**พูด (optional):** DAG `graphify_analyze_job` รันการวิเคราะห์ Graphify ทั้งก้อนเป็น pod เดียว (`graphify-airflow:local`) — เหมาะ re-scan ตาม demand"
**พูด:** "Airflow orchestrate ได้ทั้ง ETL และรายงานความซับซ้อนโค้ดรายวัน — ต่อยอด Slack/email/dashboard ได้"

## 8. Cross-team impact เมื่อแก้ shared code (2 นาที)
**พูด:** "องค์กรจริงมีหลายทีมใช้ library ร่วม — แก้ `clean_data()` ครั้งเดียวอาจทำให้ DAG ของ Sales, Marketing, Finance, Ops พังพร้อมกัน"
**จอ:** โครง `airflow/dags/common/pipeline.py` + DAG `team_sales_daily`, `team_marketing_campaign`, …
**จอ (live หรือ screenshot):** เปิด PR จาก branch `demo/team-platform-clean-data-impact` ที่แก้ `clean_data` → GitHub Action **DAG impact analysis** → sticky comment แสดงตาราง DAG/task + owner
**พูด:** "Graphify สร้างกราฟ import/call จาก AST — ไม่ใช้ LLM — แล้วรวมกับ ExternalTaskSensor (Ops รอ Sales) และ Asset (Finance ถูก trigger จาก Sales) เพื่อจับ **runtime** cross-DAG ด้วย"
**ขั้นเตรียม:** ดู README หมวด *Cross-team DAG impact* และ `docs/impact-example.md`

## 9. คุณค่าทางธุรกิจ & ขั้นต่อไป (1.5 นาที)
- **Onboarding เร็วขึ้น**: dev ใหม่เห็นแผนที่ระบบตั้งแต่วันแรก
- **ลดความเสี่ยงการเปลี่ยนแปลง**: รู้ว่าโมดูลไหนกระทบกว้าง (impact analysis)
- **มองเห็น technical debt**: circular dependency, จุดพึ่งพาสูง
- **ปลอดภัย & ต้นทุนต่ำ**: open-source, รันภายใน, ไม่ส่งโค้ดออก, ไม่มีค่า API
- **ขั้นต่อไป**: PoC กับ repo ภายในองค์กร 1-2 ระบบ, ตั้ง DAG รายสัปดาห์, เชื่อม CI

## 10. Q&A (เผื่อ 2-3 นาที)
- *ต้องใช้ AI ไหม?* — ไม่จำเป็น ส่วน code เป็น AST ล้วน; การตั้งชื่อ cluster/วิเคราะห์เอกสารใช้ LLM ได้ถ้าต้องการ (optional)
- *ใช้กับภาษาอื่นได้ไหม?* — รองรับหลายภาษา (Python, TypeScript ฯลฯ) ผ่าน tree-sitter
- *ขึ้น production ยังไง?* — manifest เดียวกันขึ้น EKS/GKE/AKS ได้ เปลี่ยน secret และใช้ Postgres managed
