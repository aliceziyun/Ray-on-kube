import random
import sys
import os
import socket
import json

HOST = '172.31.97.49'
PORT = 22222

def send_message_to_kubeserver(message):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))

        # pack message
        s.send(json.dumps(message).encode())

        # handle response
        # response = s.recv(1024)
        s.close()

if __name__ == "__main__":
    # print("[kube client] into main")
    command = ["/usr/local/lib/python3.8/site-packages/ray/_private/workers/default_worker.py"]
    command.extend(sys.argv[1:])
    command.extend([f"--startup-token","0",f"--webui","''"])

    # TODO: add ip address of current node
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
            "cmdline": command
        },
        "envs": envs,
        "name": name,
        "type": "worker"
    }

    send_message_to_kubeserver(message)
