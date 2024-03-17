import ray
import os

mode=0
plasma_store_socket_name = "/tmp/ray/session_2024-03-15_16-59-15_498793_237985/sockets/plasma_store"
raylet_socket_name = "/tmp/ray/session_2024-03-15_16-59-15_498793_237985/sockets/raylet"
job_id = ray.JobID("0000".encode('utf-8'))
logs_dir = "/tmp/ray/session_2024-03-15_16-54-04_221822_232456/logs"
node_ip_address = "172.31.97.49"
node_manager_port = 46437
raylet_ip_address = "172.31.97.49"
driver_name = "mytest/test.py"

print(job_id)

job_config = ray.job_config.JobConfig()

gcs_options = ray._raylet.GcsClientOptions.from_gcs_address("172.31.97.49:59246")
entry_point = ray._private.utils.get_entrypoint_name()
job_config_s = b'"\x0b\n\x02{}\x1a\x05\x08\xd8\x04\x10\x01*$94c1544b-9937-4ac9-b4f3-12bf4eebfa038\x01B\x1b/mnt/d/SJTU/code/ray-masterB"/mnt/d/SJTU/code/ray-master/mytest'
myworker = ray._raylet.CoreWorker(
        mode,
        plasma_store_socket_name,
        raylet_socket_name,
        job_id,
        gcs_options,
        logs_dir,
        node_ip_address,
        node_manager_port,
        raylet_ip_address,
        False,
        driver_name,
        "",
        "",
        job_config_s,
        59134,
        0,
        0,
        "session_2024-03-15_16-59-15_498793_237985",
        "0f10c042fcafcce3f6db536f3fa23e4c841f3f98f7a556a385d65596",
        entry_point,
        -1,
        -1,
    )
myworker.notify_raylet()
print(myworker)