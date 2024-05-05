''' Attribute constants user can change'''
PROXY_HOST = '172.31.97.49'
PROXY_PORT = 22222

# Kubernetes namespace of ray cluster
KUBE_NAMESPACE = "default"
# Image use to start a ray process and logger
RAY_IMAGE = "ray_test:latest"
# Max retries when connecting the kubernetes cluster
MAX_RETIRES = 5

# Default ray mount path
DEFAULT_MOUNT_STORAGE = "/tmp/ray/"
# Python executable path in container
EXECUTABLE_PATH = "/usr/local/"
# Kube logger execute path in container
LOGGER_PATH = "/usr/local/lib/python3.8/site-packages/ray/_private/kube/kube_logger.py"
# Default worker path in container
DEFAULT_WORKER_PATH = "/usr/local/lib/python3.8/site-packages/ray/_private/workers/default_worker.py"
# Container log files storage
LOG_STORAGE_PATH = "/var/lib/docker/containers/"
