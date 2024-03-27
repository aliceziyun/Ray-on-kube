import sys
import subprocess
import os
import random
import ray._private.services as ray_service
from kubernetes import client, config
from kubernetes.client.models import V1Volume, V1VolumeMount

DEFAULT_STORAGE = "/tmp/ray/"

TIME_THRESHOLD = 15

POD_RUNNING = "Running"
POD_FAILED = "Failed"

import ray._private.ray_constants as ray_constants

def _get_env_list():
    """Get all required environmental variables of the newly created pod."""
    env_list = []
    
    job_id_env = client.V1EnvVar(name="RAY_JOB_ID", value=os.getenv("RAY_JOB_ID"))
    env_list.append(job_id_env)
    raylet_pid_env = client.V1EnvVar(name="RAY_RAYLET_PID", value=os.getenv("RAY_RAYLET_PID"))
    env_list.append(raylet_pid_env)
    for env_var_name, env_var_value in os.environ.items():
            if env_var_name.startswith("RAY_") and env_var_name not in [
                "RAY_RAYLET_PID",
                "RAY_JOB_ID",
            ]:
                new_env = client.V1EnvVar(name=env_var_name, value=env_var_value)
                env_list.append(new_env)
    
    pod_ip_env = client.V1EnvVar(
        name="POD_IP",
        value_from=client.V1EnvVarSource(
            field_ref=client.V1ObjectFieldSelector(
                field_path="status.podIP"
            )
        )
    )
    env_list.append(pod_ip_env)

    return env_list

def get_ip_address(args):
    for arg in args:
        if arg.startswith("--node-ip-address="):
            return arg.split("=")[1]

def create_default_worker_pod(args,ip):
    """Create a pod and add it to the k8s node.
    Return the name of newly created pod. 
    
    About the image: use the ray_test built by myself locally"""

    config.load_kube_config()
    v1 = client.CoreV1Api()
    mount_dir = DEFAULT_STORAGE

    # create mount path
    volume = V1Volume(
        name="worker-volume",
        host_path=client.V1HostPathVolumeSource(path=mount_dir),
    )
    volume_mount = V1VolumeMount(
        name="worker-volume",
        mount_path=mount_dir,
    )

    # set environment variables
    env_list = _get_env_list()

    # create pod
    print("[kube] creating a default worker pod")
    name = ip+"-worker-"+str(random.randint(0, 100000)).zfill(6)
    pod = client.V1Pod()
    pod.metadata = client.V1ObjectMeta(name=name)
    pod.spec = client.V1PodSpec(
        containers=[client.V1Container(
                name="raytest", 
                image="ray_test:latest",
                image_pull_policy="Never",
                volume_mounts=[volume_mount],
                env=env_list,
                security_context=client.V1SecurityContext(privileged=True),
                command=["python"],
                args=args,
                restart_policy="Never"
                )
            ],
        restart_policy="Never",
        host_users=True,
        host_pid=True,
        host_ipc=True,
        host_network=True,
        volumes=[volume],
    )
    v1.create_namespaced_pod(namespace="default", body=pod)

if __name__ == "__main__":
    print("[pod worker] into main")
    command = ["/usr/local/lib/python3.8/site-packages/ray/_private/workers/default_worker.py"]
    command.extend(sys.argv[1:])
    command.extend([f"--startup-token","0",f"--webui","''"])
    print("[pod worker] command is: ", command)

    create_default_worker_pod(command,get_ip_address(sys.argv[1:]))