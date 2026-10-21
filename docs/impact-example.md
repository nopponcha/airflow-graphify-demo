# ตัวอย่างผล DAG impact analysis (local)

สถานการณ์จำลอง: **Team Platform** แก้ `clean_data()` ใน `airflow/dags/common/pipeline.py` (reject negative amounts)  
คำสั่ง:

```bash
pip install graphifyy==0.9.92 pyyaml
git checkout -b demo/team-platform-clean-data-impact   # บนเครื่องจริง
# ... แก้ pipeline.py แล้ว commit ...
python scripts/impact_analysis.py --base origin/main --head HEAD --output-md /tmp/impact.md
```

ช่วง diff ที่ใช้สร้างตัวอย่างนี้ใน repo: `HEAD~1..HEAD` หลัง commit `platform: reject negative amounts in clean_data`.

---

## DAG impact analysis

| Changed symbol / file | Affected DAG | Task | Owner | Link type |
| --- | --- | --- | --- | --- |
| `clean_data (dags/common/pipeline.py)` | `team_ops_monitoring` | `—` | Team Ops (@nopponcha) | code import |
| `clean_data (dags/common/pipeline.py)` | `team_sales_daily` | `—` | Team Sales (@nopponcha) | code import |
| `clean_data (dags/common/pipeline.py)` | `team_finance_report` | `—` | Team Finance (@nopponcha) | code import |
| `clean_data (dags/common/pipeline.py)` | `team_finance_report` | `enrich` | Team Finance (@nopponcha) | code import |
| `clean_data (dags/common/pipeline.py)` | `team_marketing_campaign` | `team_marketing_campaign` | Team Marketing (@nopponcha) | code import |
| `clean_data (dags/common/pipeline.py)` | `team_marketing_campaign` | `clean` | Team Marketing (@nopponcha) | code import |
| `clean_data (dags/common/pipeline.py)` | `team_sales_daily` | `team_sales_daily` | Team Sales (@nopponcha) | code import |
| `clean_data (dags/common/pipeline.py)` | `team_ops_monitoring` | `normalize` | Team Ops (@nopponcha) | code import |
| `clean_data (dags/common/pipeline.py)` | `team_sales_daily` | `clean` | Team Sales (@nopponcha) | code import |
| `clean_data (dags/common/pipeline.py)` | `team_sales_daily` | `enrich` | Team Sales (@nopponcha) | code import |
| `clean_data (dags/common/pipeline.py)` | `team_marketing_campaign` | `—` | Team Marketing (@nopponcha) | code import |
| `clean_data (dags/common/pipeline.py)` | `team_finance_report` | `team_finance_report` | Team Finance (@nopponcha) | code import |
| `clean_data (dags/common/pipeline.py)` | `team_marketing_campaign` | `load` | Team Marketing (@nopponcha) | code import |
| `clean_data (dags/common/pipeline.py)` | `team_ops_monitoring` | `team_ops_monitoring` | Team Ops (@nopponcha) | code import |
| `clean_data (dags/common/pipeline.py)` | `team_sales_daily` | `load_warehouse` | Team Sales (@nopponcha) | code import |
| `clean_data (dags/common/pipeline.py)` | `team_marketing_campaign` | `validate` | Team Marketing (@nopponcha) | code import |
| `clean_data (dags/common/pipeline.py)` | `team_finance_report` | `clean` | Team Finance (@nopponcha) | code import |
| `(runtime cross-DAG)` | `team_ops_monitoring` | `wait_team_sales_daily.load_warehouse` (ExternalTaskSensor → team_sales_daily.load_warehouse) | Team Ops (@nopponcha) | runtime |
| `(runtime cross-DAG)` | `team_finance_report` | `(asset schedule)` (Asset file://demo/warehouse/sales_daily from team_sales_daily) | Team Finance (@nopponcha) | runtime |

### Owners to notify

- Team Finance (@nopponcha)
- Team Marketing (@nopponcha)
- Team Ops (@nopponcha)
- Team Sales (@nopponcha)

---

**หมายเหตุ:** แถว `code import` มาจาก Graphify reverse walk + AST imports; แถว `runtime` มาจาก ExternalTaskSensor / Asset เมื่อ DAG ต้นทาง (เช่น Sales) อยู่ในชุดที่ได้รับผลจาก shared code
