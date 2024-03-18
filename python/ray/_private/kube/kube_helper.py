import time
import ray._private.kube.pod as pod
from kubernetes import client, config
from enum import Enum

class KubeType(Enum):
    CREATE_WORKER = 1

class Kubehelper():
    """Belong to a ray node, help to create pod and get pod's information."""
    def __init__(
            self,
            namespace: str = "default",
            node_name: str = ""
    ):
        """Initialize a Kube helper."""
        print("[dev] init a kube helper")
        self.pod_list = {}
        self.v1 = client.CoreV1Api()
        self.namespace = namespace
        self.node_name = node_name

        config.load_kube_config("admin.conf")

    def pod_create(self,type,message):
        """Create pod according to the specific type."""
        if type == KubeType.CREATE_WORKER:
            """Create a coreworker and return its ip address."""
            worker_pod = pod.create_pod(self, message)
            worker_pod_ip = pod.get_pod_ip_address(worker_pod)
            self.pod_list[worker_pod] = worker_pod_ip
            return worker_pod_ip
        else:
            print("[dev error] no such kube type!")
