from concurrent import futures
import logging
import time

import grpc
import ray.grpc_test.worker_pb2 as worker_pb2
import ray.grpc_test.worker_pb2_grpc as worker_pb2_grpc

import ray

SCRIPT_MODE = 0
WORKER_MODE = 1
LOCAL_MODE = 2
SPILL_WORKER_MODE = 3
RESTORE_WORKER_MODE = 4

c_worker = None

class CoreWorkerInfo(worker_pb2_grpc.WorkerService):
    def Info(self, request, context):
        print("[core worker] start a new worker")
        mode = request.mode
        plasma_store_socket_name = request.plasma_store_socket_name
        raylet_socket_name = request.raylet_socket_name
        gcs_address = request.gcs_address
        logs_dir = request.logs_dir
        node_ip_address = request.node_ip_address
        node_manager_port = request.node_manager_port
        raylet_ip_address = request.raylet_ip_address
        local_mode = request.local_mode
        driver_name = request.driver_name
        log_stdout_file_path = request.log_stdout_file_path
        log_stderr_file_path = request.log_stderr_file_path
        serialized_job_config = request.serialized_job_config
        metrics_agent_port = request.metrics_agent_port
        runtime_env_hash = request.runtime_env_hash
        startup_token = request.startup_token
        session_name = request.session_name
        cluster_id = request.cluster_id
        entry_point = request.entry_point
        worker_launch_time_ms = request.worker_launch_time_ms
        worker_launched_time_ms = request.worker_launched_time_ms
        job_binary = request.job_binary

        gcs_options = ray._raylet.GcsClientOptions.from_gcs_address(gcs_address)
        job_id = ray.JobID(job_binary)
        
        global c_worker
        c_worker = ray._raylet.CoreWorker(
            mode,
            plasma_store_socket_name,
            raylet_socket_name,
            job_id,
            gcs_options,
            logs_dir,
            node_ip_address,
            node_manager_port,
            raylet_ip_address,
            local_mode,
            driver_name,
            log_stdout_file_path,
            log_stderr_file_path,
            serialized_job_config,
            metrics_agent_port,
            runtime_env_hash,
            startup_token,
            session_name,
            cluster_id,
            "" if mode != SCRIPT_MODE else entry_point,
            worker_launch_time_ms,
            worker_launched_time_ms,
        )
        c_worker.notify_raylet()
        return worker_pb2.CoreworkerResponse(message="successful")
		

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    worker_pb2_grpc.add_WorkerServiceServicer_to_server(CoreWorkerInfo(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
	logging.basicConfig()
	serve()
