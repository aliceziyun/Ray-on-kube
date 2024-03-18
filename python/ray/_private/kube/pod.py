import time
from kubernetes import client, config

TIME_THRESHOLD = 15

POD_RUNNING = "Running"
POD_FAILED = "Failed"

def create_pod(kube_helper, job_id):
    """Create a pod and add it to the k8s node.
    Return the name of newly created pod. 
    
    About the image: use the ray_test built by myself locally"""

    print("[kube] creating a worker pod with job_id ", job_id.binary())

    v1 = kube_helper.v1

    pod_name = kube_helper.node_name + "_" + job_id.binary()

    pod = client.V1Pod()
    pod.metadata = client.V1ObjectMeta(name=pod_name)
    pod.spec = client.V1PodSpec(containers=[client.V1Container(
            name="raytest", 
            image="ray_test:latest",
            image_pull_policy="Never"
            )])
    created_pod = v1.create_namespaced_pod(namespace=kube_helper.namespace, body=pod)
    return created_pod.metadata.name


def get_pod_ip_address(kube_helper, pod_name):
    """Get pod's ipaddress with NAME """
    v1 = kube_helper.v1

    time_passed = 0
    while True:
        if time_passed > TIME_THRESHOLD:
            """Wait for more than 15 seconds, something must went wrong."""
            print("[kube] wait for pod %s to running over 15 sec" % pod_name)
            exit(-1)

        pod_info = v1.read_namespaced_pod_status(name=pod_name, namespace=kube_helper.namespace)

        if pod_info.status.phase == POD_RUNNING:
            pod_ip = pod_info.status.pod_ip
            return pod_ip
        elif pod_info.status.phase == POD_FAILED:
            print("[kube] Pod %s failed" % pod_name)
            exit(-1)
        else:
            print("[kube] Pod %s is not running yet. Waiting..." % pod_name)
            time_passed += 2
            time.sleep(2)