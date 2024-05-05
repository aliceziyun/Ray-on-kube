import json
import subprocess
import sys
import os
import time

import ray._private.kube.kube_config as kuberay_config
import ray._private.kube.kube_client as kube_client

def get_main_container_id(pod_name):
    message = {
        "command": "get_mainid",
        "name": pod_name,
    }
    raw_id = kube_client.send_message_to_kubeproxy(message).decode()
    return raw_id[len("docker://"):]

def kube_logger_redirect(stdout_f, stderr_f, container_id):
    with open(stdout_f, "a") as outfile, open(stderr_f, "a") as errfile:
        log_file_path = os.join(kuberay_config.LOG_STORAGE_PATH, f"{container_id}/{container_id}-json.log")

        tail_process = subprocess.Popen(['tail', '-f', log_file_path], stdout=subprocess.PIPE)

        while True:
            line = tail_process.stdout.readline().decode().strip()

            if not line:
                time.sleep(1)
                continue

            try:
                json_data = json.loads(line)
            except json.JSONDecodeError:
                continue
                
            if "stream" in json_data:
                stream = json_data["stream"]
                if stream == "stdout":
                    outfile.write(json_data["log"])
                if stream == "stderr":
                    errfile.write(json_data["log"])


if __name__ == "__main__":
    stdout_file, stderr_file, pod_name = sys.argv[1:4]

    main_container_id = get_main_container_id(pod_name)
    # print("[kube logger]", main_container_id)

    kube_logger_redirect(stdout_file, stderr_file, main_container_id)