import random
import sys
import os
import socket
import json

import ray._private.kube.kube_config as kuberay_config

def send_message_to_kubeproxy(message):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((kuberay_config.PROXY_HOST, kuberay_config.PROXY_PORT))

        # pack message and send to proxy
        s.send(json.dumps(message).encode())

        # handle response
        response = b""
        while True:
            chunk = s.recv(1024)
            if not response:
                break
            response += chunk

        s.close()

        if response.strip() == b"success":
            return
        elif len(response) != 0:
            return response

if __name__ == "__main__":
    command = [kuberay_config.DEFAULT_WORKER_PATH] \
            + sys.argv[1:] \
            # + [f"--startup-token 0", "--webui ''"]

    name = "worker-"+str(random.randint(0, 100000)).zfill(6)

    def get_envs():
        env_dict = {}
        for env_var_name, env_var_value in os.environ.items():
            if env_var_name.startswith("RAY_"):
                env_dict[env_var_name] = env_var_value
        return env_dict
    envs = get_envs()

    message = {
        "command": "create",
        "params": {
            "cmdline": command,
        },
        "envs": envs,
        "name": name,
        "type": "worker"
    }

    send_message_to_kubeproxy(message)
