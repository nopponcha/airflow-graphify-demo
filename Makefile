.PHONY: start build deploy ui graph watch-pods validate teardown
start:    ## start minikube sized for a laptop
	minikube start --cpus 4 --memory 8g --driver=docker
build:
	./scripts/build.sh
deploy:
	./scripts/deploy.sh
ui:
	kubectl -n airflow-demo port-forward svc/airflow-api-server 8080:8080
graph:
	kubectl -n airflow-demo port-forward svc/graphify 8081:8080
watch-pods:
	kubectl get pods -n airflow-demo -w
validate:
	python3 -m py_compile airflow/dags/*.py
	command -v kubeconform >/dev/null && kubeconform -strict -summary k8s/*.yaml || echo "skip kubeconform (not installed)"
teardown:
	./scripts/teardown.sh
