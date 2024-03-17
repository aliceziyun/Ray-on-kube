import grpc
import ray.grpc_test.worker_pb2 as worker_pb2
import ray.grpc_test.worker_pb2_grpc as worker_pb2_grpc

# 创建 CoreworkerRequest 对象
request = worker_pb2.CoreworkerRequest()

# 设置字段值
request.mode = 1
request.plasma_store_socket_name = "plasma_store_socket_name_value"
request.raylet_socket_name = "raylet_socket_name_value"
request.gcs_address = "gcs_address_value"
request.logs_dir = "logs_dir_value"
request.node_ip_address = "node_ip_address_value"
request.node_manager_port = 12345
request.raylet_ip_address = "raylet_ip_address_value"
request.local_mode = True
request.driver_name = "driver_name_value"
request.log_stdout_file_path = "log_stdout_file_path_value"
request.log_stderr_file_path = "log_stderr_file_path_value"
request.serialized_job_config = b"serialized_job_config_value"
request.metrics_agent_port = 6789
request.runtime_env_hash = 9876
request.startup_token = 123
request.session_name = "session_name_value"
request.cluster_id = "cluster_id_value"
request.entry_point = "entry_point_value"
request.worker_launch_time_ms = 123456
request.worker_launched_time_ms = 789012

# 将 request 对象传递给 gRPC stub 的 Info 方法
with grpc.insecure_channel('localhost:50051') as channel:
    stup = worker_pb2_grpc.WorkerServiceStub(channel)
    response = stup.Info(request)