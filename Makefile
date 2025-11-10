.PHONY: help build test docker-build docker-push k8s-deploy k8s-delete clean

# Variables
DOCKER_REGISTRY ?= your-registry
IMAGE_NAME ?= dolda
VERSION ?= latest
NAMESPACE ?= dolda

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Local Development
build: ## Build the project in release mode
	cargo build --release

test: ## Run all tests
	cargo test --release

bench: ## Run benchmarks
	cargo bench

stress-test: ## Run stress tests
	cargo test --release --test storage_stress -- --test-threads=1 --nocapture

# Docker
docker-build: ## Build Docker image
	docker build -t $(IMAGE_NAME):$(VERSION) .
	docker tag $(IMAGE_NAME):$(VERSION) $(IMAGE_NAME):latest

docker-push: ## Push Docker image to registry
	docker tag $(IMAGE_NAME):$(VERSION) $(DOCKER_REGISTRY)/$(IMAGE_NAME):$(VERSION)
	docker push $(DOCKER_REGISTRY)/$(IMAGE_NAME):$(VERSION)
	docker push $(DOCKER_REGISTRY)/$(IMAGE_NAME):latest

docker-run: ## Run single Docker container
	docker run -d --name dolda \
		-p 8080:8080 -p 8081:8081 -p 9090:9090 \
		-v dolda-data:/data -v dolda-logs:/logs \
		$(IMAGE_NAME):$(VERSION)

docker-stop: ## Stop and remove Docker container
	docker stop dolda || true
	docker rm dolda || true

# Docker Compose
compose-up: ## Start docker-compose cluster
	docker-compose up -d

compose-down: ## Stop docker-compose cluster
	docker-compose down

compose-logs: ## View docker-compose logs
	docker-compose logs -f

compose-ps: ## Show docker-compose status
	docker-compose ps

compose-clean: ## Clean up docker-compose (including volumes)
	docker-compose down -v
	docker volume prune -f

# Kubernetes
k8s-deploy: ## Deploy to Kubernetes
	kubectl apply -f k8s/

k8s-deploy-kustomize: ## Deploy using kustomize
	kubectl apply -k k8s/

k8s-delete: ## Delete from Kubernetes
	kubectl delete -f k8s/

k8s-status: ## Check Kubernetes deployment status
	kubectl get all -n $(NAMESPACE)

k8s-pods: ## Show Kubernetes pods
	kubectl get pods -n $(NAMESPACE) -o wide

k8s-logs: ## Show logs from first pod
	kubectl logs -n $(NAMESPACE) dolda-0 -f

k8s-exec: ## Execute shell in first pod
	kubectl exec -it -n $(NAMESPACE) dolda-0 -- /bin/bash

k8s-port-forward: ## Port forward to first pod
	kubectl port-forward -n $(NAMESPACE) dolda-0 8080:8080

k8s-describe: ## Describe first pod
	kubectl describe pod -n $(NAMESPACE) dolda-0

k8s-events: ## Show Kubernetes events
	kubectl get events -n $(NAMESPACE) --sort-by='.lastTimestamp'

k8s-scale: ## Scale StatefulSet (use REPLICAS=5)
	kubectl scale statefulset dolda -n $(NAMESPACE) --replicas=$(REPLICAS)

# Monitoring
metrics: ## Show metrics from local instance
	curl http://localhost:9090/metrics

health: ## Check health of local instance
	curl http://localhost:8081/health

prometheus: ## Open Prometheus UI
	open http://localhost:9093

grafana: ## Open Grafana UI (docker-compose)
	@echo "Grafana: http://localhost:3000 (admin/admin)"
	open http://localhost:3000

# Cleanup
clean: ## Clean build artifacts
	cargo clean
	rm -rf target/

clean-all: clean compose-clean ## Clean everything including Docker volumes
	docker system prune -af
	docker volume prune -f

# CI/CD
ci-test: ## Run CI tests
	cargo test --release --all-features
	cargo clippy -- -D warnings
	cargo fmt -- --check

ci-build: docker-build ## Build for CI

ci-deploy: docker-build docker-push ## Full CI/CD pipeline

# Development helpers
fmt: ## Format code
	cargo fmt

lint: ## Run clippy
	cargo clippy -- -D warnings

check: ## Run cargo check
	cargo check --all-features

doc: ## Generate documentation
	cargo doc --no-deps --open

# Quick commands
dev: build test ## Build and test
deploy: docker-build k8s-deploy ## Build and deploy to k8s
full-deploy: docker-build docker-push k8s-deploy ## Full deployment pipeline

