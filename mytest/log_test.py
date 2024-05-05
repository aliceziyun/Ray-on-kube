from kubernetes import client, config
from kubernetes.client.models import V1Volume, V1VolumeMount

config.load_kube_config()
tclient = client.CoreV1Api()
namespace = "default"

pod = client.V1Pod()
name = "test"
sidecar_args =["/home/alice/anaconda3/envs/basement/lib/python3.8/site-packages/ray/_private/kube/kube_logger.py", "/tmp/ray/testout.txt", "/tmp/ray/testerr.txt", name]
pod.metadata = client.V1ObjectMeta(name=name)
pod.spec = client.V1PodSpec(
    containers=[
        client.V1Container(
            name="container-"+name, 
            image="ray_test:latest",
            image_pull_policy="Never",
            security_context=client.V1SecurityContext(privileged=True,run_as_user=1000,run_as_group=1000),
            restart_policy="Never",
            ),
        client.V1Container(
            name="sidecar-"+name, 
            image="ray_test:latest",
            image_pull_policy="Never",
            security_context=client.V1SecurityContext(privileged=True,run_as_user=1000,run_as_group=1000),
            command=["python"],
            args=sidecar_args,
            restart_policy="Never"
            ),
        ],
    restart_policy="Never",
    host_users=True,
    host_pid=True,
    host_ipc=True,
    host_network=True,
)
tclient.create_namespaced_pod(namespace=namespace, body=pod)
