# Monitoring

## Setting up Prometheus
### Prerequisites
- [Follow the instructions](https://github.com/prometheus-operator/prometheus-operator#quickstart) to install the Prometheus operator.

### Launch
Monitoring services are created by the {{product-name}} operator automatically. To collect metrics, you need to:
1. Create [ServiceMonitor](https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/prometheus/prometheus_service_monitor.yaml).
2. Create a [service account](https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/prometheus/prometheus_service_account.yaml).
3. Give the account you created a [role](https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/prometheus/prometheus_role_binding.yaml).
4. [Create Prometheus](https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/prometheus/prometheus.yaml).
