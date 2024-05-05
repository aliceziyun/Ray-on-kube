# def start_raylet_container():
import sys
import psutil
import time
import os
from kubernetes import client, config
from kubernetes.client.models import V1Volume, V1VolumeMount
from kubernetes.client.rest import ApiException

DEFAULT_STORAGE = "/tmp/ray/"

def get_pid():
    while True:
        for proc in psutil.process_iter(['pid', 'cmdline']):
            try:
                cmdline = proc.info['cmdline']
                
                if any("raylet" in arg for arg in cmdline):
                    return proc.pid
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        time.sleep(1)

def get_env_list(envs):
    env_list = []
    for name,value in envs.items():
        env_list.append(client.V1EnvVar(name=name, value=value))
    env_list = env_list[-3:]

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
    
    print(env_list)

    return env_list

def create_new_pod(command,envs,name):
    config.load_kube_config()
    v1 = client.CoreV1Api()
    mount_dir = DEFAULT_STORAGE

    volume = V1Volume(
        name="worker-volume",
        host_path=client.V1HostPathVolumeSource(path=mount_dir),
    )
    volume_mount = V1VolumeMount(
        name="worker-volume",
        mount_path=mount_dir,
    )

    env_list = get_env_list(envs)

    # create pod
    print("[kube] creating a normal pod")
    exit(0)

    pod = client.V1Pod()
    pod.metadata = client.V1ObjectMeta(name=name)
    pod.spec = client.V1PodSpec(
        containers=[client.V1Container(
                name="raytest", 
                image="ray_test:latest",
                image_pull_policy="Never",
                volume_mounts=[volume_mount],
                env=env_list,
                security_context=client.V1SecurityContext(privileged=True,run_as_user=1000,run_as_group=1000),
                # command=["/bin/bash", "-c"],
                args=command,
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
    modified_env = eval(sys.argv[1].replace("/home/alice/anaconda3/envs/basement/lib/","/usr/local/lib/"))
    stdout_file = sys.argv[2]
    stderr_file = sys.argv[3]
    pipe_stdin = sys.argv[4]
    pod_name = sys.argv[5]
    command = sys.argv[6:]
    # replace the path of the python file in container
    command_list = [cmd.replace("/home/alice/anaconda3/envs/basement/lib/","/usr/local/lib/") for cmd in command]
    command_list = [cmd.replace("/home/alice/anaconda3/envs/basement/bin/python","/usr/local/bin/python3") for cmd in command_list]

    # print("[pod_process_starter]: ",command_list)

    create_new_pod(command_list,modified_env,pod_name)
    