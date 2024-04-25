import socket
import threading
import json
import os
import sys
from kubernetes import client, config
from kubernetes.client.models import V1Volume, V1VolumeMount

HOST = '172.31.97.49'
PORT = 22222

DEFAULT_MOUNT_STORAGE = "/tmp/ray/"

EXECUTABLE_PATH = "/usr/local/"

class KubeServer:
    def __init__(self):
        '''Initialize a Kube server object.'''
        config.load_kube_config()

        self.v1_client = client.CoreV1Api()
        self.namespace = "default"
    
    def run_server(self):
        # loop and listen to new request
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((HOST, PORT))
            s.listen()
            while True:
                conn, addr = s.accept()
                threading.Thread(target=self.handle_client, args=(conn, addr)).start()
    
    def handle_client(self, conn, addr):
        while True:
            data = conn.recv(4096)
            # print("[kube server] receive data:", data)
            if len(data) == 0:      # don't know why this will happen
                break
            message = dict(json.loads(data.decode()))
            command = message.get("command")
            if command == 'create':
                name = message.get("name")
                params = message.get("params", {})
                envs = message.get("envs")
                type = message.get("type")
                self.create_pod(name, envs, params.get("cmdline"), type)
            elif command == 'kill':
                type = message.get("type")
                params = message.get("params", {})
                self.delete_pod(type, params)
            elif command == 'killall':
                self.delete_all_pod()

            message = "ok"
            conn.sendall(message.encode())

    def create_pod(self, name, envs, command, type):
        # determine the start cmd in container
        start_cmd = []
        if type == "worker":
            start_cmd.append("python")

        # replace the python executable path to path in container
        def replace_exe_path(cmd, env):
            cmd = [cmd.replace("/home/alice/anaconda3/envs/basement/", EXECUTABLE_PATH) for cmd in command]
            env = eval(str(env).replace("/home/alice/anaconda3/envs/basement/", EXECUTABLE_PATH))
            return cmd, env
        command, envs = replace_exe_path(command, envs)

        # print("[kube server] start pod command is: ",command)

        # make mount directory
        mount_dir = DEFAULT_MOUNT_STORAGE
        volume = V1Volume(
            name="worker-volume",
            host_path=client.V1HostPathVolumeSource(path=mount_dir),
        )
        volume_mount = V1VolumeMount(
            name="worker-volume",
            mount_path=mount_dir,
        )

        # set environments
        def get_env_list(envs):
            env_list = []
            if envs != None:
                for name,value in envs.items():
                    env_list.append(client.V1EnvVar(name=name, value=value))

            # print(env_list)

            return env_list

        env_list = get_env_list(envs)
        
        # create pod
        print("[kube] creating a pod")
        
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
                    command=start_cmd,
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
        self.v1_client.create_namespaced_pod(namespace=self.namespace, body=pod)

    def delete_pod(self, type, params):
        # print("[kube server] delete type is: ", type)
        name = type.replace("_","-")
        try:
            self.v1_client.delete_namespaced_pod(name, self.namespace)
            print("[kube server] delete pod %s\n" % name)
        except Exception as e:
            print("Exception when deleting Pod: %s\n" % e)

    def delete_all_pod(self):
        try:
            pods_list = self.v1_client.list_namespaced_pod(self.namespace)

            for pod in pods_list.items:
                pod_name = pod.metadata.name
                self.delete_pod(pod_name, self.namespace)
        except Exception as e:
            print("Exception when deleting Pods: %s\n" % e)