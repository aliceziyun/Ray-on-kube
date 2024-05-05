import socket
import threading
import json
import time
import sys
import ray._private.kube.kube_config as kuberay_config
from kubernetes import client, config
from kubernetes.client.models import V1Volume, V1VolumeMount

class KubeProxy:
    ''' Running within a Ray cluster, used to communicate with the Kubernetes server.

    Attributes:
            kube_server: Kubernetes default server
            namespace: Kubernetes namespace
            exec_path: The installation path of the current Python interpreter.
    '''
    def __init__(self):
        '''Initialize a Kube server object.'''
        config.load_kube_config()

        self.kube_server = client.CoreV1Api()
        self.namespace = kuberay_config.KUBE_NAMESPACE
        self.py_path = sys.prefix
    
    def run_server(self):
        ''' Start the kube server. '''
        # loop and listen to new request
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((kuberay_config.PROXY_HOST, kuberay_config.PROXY_PORT))
            s.listen()
            while True:
                conn, addr = s.accept()
                threading.Thread(target=self.handle_client, args=(conn, addr)).start()
    
    def handle_client(self, conn, addr):
        ''' Continuously listens for incoming messages from clients and send the message
            to kubernetes Server.
        
        Message Structure:
        {
            "command": str,         # Command to be executed ('create', 'kill', 'killall', 'get_mainid')
            "name": str,            # Name of the pod
            "params": dict,         # Additional parameters
            "envs": list,           # Environment variables
            "type": str             # Type of pod operation
        }

        - command: Defines the action to be performed on the pod(s).
            - 'create': Creates a new pod.
            - 'kill': Deletes a pod.
            - 'killall': Deletes all pods.
            - 'get_mainid': Retrieves the main container ID of a pod.
        '''
        while True:
            data = conn.recv(4096)
            # print("[kube server] receive data:", data)
            if len(data) == 0:
                break
            message = dict(json.loads(data.decode()))
            message_send_back = "success"
            command = message.get("command")
            if command == 'create':
                name = message.get("name")
                params = message.get("params", {})
                envs = message.get("envs")
                type = message.get("type")
                self.create_pod(name, envs, params, type)
            elif command == 'kill':
                type = message.get("type")
                params = message.get("params", {})
                self.delete_pod(type, params)
            elif command == 'killall':
                self.delete_all_pod()
            elif command == 'get_mainid':
                pod_name = message.get("name")
                message_send_back = str(self.get_main_container_id_by_name(pod_name))
            conn.sendall(message_send_back.encode())

    def create_pod(self, name, envs, params, type):
        ''' Create a pod to run the ray process. 
        
        Pod:
            - main container: use to start a ray process
            - sidecar containter: use to collect logs of main container
        '''
        # check whether the pod exists
        if self.get_pod_by_name(name, False) != None:
            return

        command = params.get("cmdline")

        # worker or other process
        start_cmd = []
        if type == "worker":
            start_cmd.append("python")

        # replace the python executable path to path in container
        def replace_exe_path(cmd, env):
            cmd = [cmd.replace(self.py_path, kuberay_config.EXECUTABLE_PATH) for cmd in command]
            env = eval(str(env).replace(self.py_path, kuberay_config.EXECUTABLE_PATH))
            return cmd, env
        command, envs = replace_exe_path(command, envs)

        # make mount directory of main container
        mount_dir = kuberay_config.DEFAULT_MOUNT_STORAGE
        volume = V1Volume(
            name="ray-volume",
            host_path=client.V1HostPathVolumeSource(path=mount_dir),
        )
        volume_mount = V1VolumeMount(
            name="ray-volume",
            mount_path=mount_dir,
        )

        # set environments
        def get_env_list(envs):
            env_list = []
            if envs != None:
                for name,value in envs.items():
                    env_list.append(client.V1EnvVar(name=name, value=value))
            return env_list

        env_list = get_env_list(envs)

        main_container = client.V1Container(
            name="container-"+name, 
            image=kuberay_config.RAY_IMAGE,
            image_pull_policy="Never",
            volume_mounts=[volume_mount],
            env=env_list,
            security_context=client.V1SecurityContext(privileged=True,run_as_user=1000,run_as_group=1000),
            command=start_cmd,
            args=command,
            restart_policy="Never"
        )

        container_list = [main_container]
        volume_list = [volume]

        # sidecar container
        if type != "worker":
            log_volume = V1Volume(
                name="log-volume",
                host_path=client.V1HostPathVolumeSource(path=kuberay_config.LOG_STORAGE_PATH),
            )
            log_volume_mount = V1VolumeMount(
                name="log-volume",
                mount_path=kuberay_config.LOG_STORAGE_PATH,
            )
            sidecar_args =[kuberay_config.LOGGER_PATH, params.get("stdout"), params.get("stderr"), name]

            sidecar_container = client.V1Container(
                name="sidecar-"+name, 
                image=kuberay_config.RAY_IMAGE,
                image_pull_policy="Never",
                volume_mounts=[volume_mount,log_volume_mount],
                security_context=client.V1SecurityContext(privileged=True),
                command=["python"],
                args=sidecar_args,
                restart_policy="Never"
            )
            # container_list.append(sidecar_container)
            # volume_list.append(log_volume)
        
        # create pod
        print("[kube] creating a pod", name)
        pod = client.V1Pod()
        pod.metadata = client.V1ObjectMeta(name=name)
        pod.spec = client.V1PodSpec(
            containers=container_list,
            restart_policy="Never",
            host_users=True,
            host_pid=True,
            host_ipc=True,
            host_network=True,
            volumes=volume_list,
        )
        self.kube_server.create_namespaced_pod(namespace=self.namespace, body=pod)

    def delete_pod(self, type, params):
        ''' Delete a pod according to name. '''
        name = type.replace("_","-")
        try:
            self.kube_server.delete_namespaced_pod(name, self.namespace)
            print("[kube server] delete pod %s" % name)
        except Exception as e:
            print("Exception when deleting Pod: %s" % e)

    def delete_all_pod(self):
        ''' Delete all pod in ray cluster '''
        try:
            pods_list = self.kube_server.list_namespaced_pod(self.namespace)

            for pod in pods_list.items:
                pod_name = pod.metadata.name
                self.delete_pod(pod_name, self.namespace)
        except Exception as e:
            print("Exception when deleting Pods: %s" % e)

    def get_pod_by_name(self, pod_name, assume_exists):
        ''' Get a pod structure with pod name. Will retry if ASSUME_EXISTS
            is True. '''
        count = 0
        while True:
            if count > kuberay_config.MAX_RETIRES:
                return None
            try:
                pod = self.kube_server.read_namespaced_pod(name=pod_name, namespace=self.namespace)
                return pod
            except client.rest.ApiException as e:
                if not assume_exists:
                    return None
                count += 1
                time.sleep(1)

    def get_main_container_id_by_name(self, pod_name):
        ''' Retrieves the main container ID of a pod.'''
        pod = self.get_pod_by_name(pod_name, True)
        if pod == None:
            return "No Such Pod."
        for container_status in pod.status.container_statuses:
            if container_status.name == "container-"+pod_name:
                # print(container_status)
                return container_status.container_id
        return "Error"