import time
from dataclasses import dataclass, field, fields
from typing import TYPE_CHECKING, Any, Dict, Optional

import ray
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data._internal.memory_tracing import trace_allocation

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )


@dataclass
class RunningTaskInfo:
    inputs: RefBundle
    num_outputs: int
    bytes_outputs: int


@dataclass
class OpRuntimeMetrics:
    """Runtime metrics for a PhysicalOperator.

    Metrics are updated dynamically during the execution of the Dataset.
    This class can be used for either observablity or scheduling purposes.

    DO NOT modify the fields of this class directly. Instead, use the provided
    callback methods.

    Metric metadata attributes:
    - description (required): A human-readable description of the metric, also used as
        the chart description on the Ray Data dashboard.
    - metrics_group (required): The group of the metric, used to organize metrics
        into groups in StatsActor and on the Ray Data dashboard.
    - map_only (optional): Whether the metric is only measured for MapOperators.
    """

    # TODO(hchen): Fields tagged with "map_only" currently only work for MapOperator.
    # We should make them work for all operators by unifying the task execution code.

    # === Inputs-related metrics ===
    num_inputs_received: int = field(
        default=0,
        metadata={
            "description": "Number of input blocks received by operator.",
            "metrics_group": "inputs",
        },
    )
    bytes_inputs_received: int = field(
        default=0,
        metadata={
            "description": "Byte size of input blocks received by operator.",
            "metrics_group": "inputs",
        },
    )
    num_task_inputs_processed: int = field(
        default=0,
        metadata={
            "description": (
                "Number of input blocks that operator's tasks "
                "have finished processing."
            ),
            "metrics_group": "inputs",
            "map_only": True,
        },
    )
    bytes_task_inputs_processed: int = field(
        default=0,
        metadata={
            "description": (
                "Byte size of input blocks that operator's tasks "
                "have finished processing."
            ),
            "metrics_group": "inputs",
            "map_only": True,
        },
    )
    bytes_inputs_of_submitted_tasks: int = field(
        default=0,
        metadata={
            "description": "Byte size of input blocks passed to submitted tasks.",
            "metrics_group": "inputs",
            "map_only": True,
        },
    )

    # === Outputs-related metrics ===
    num_task_outputs_generated: int = field(
        default=0,
        metadata={
            "description": "Number of output blocks generated by tasks.",
            "metrics_group": "outputs",
            "map_only": True,
        },
    )
    bytes_task_outputs_generated: int = field(
        default=0,
        metadata={
            "description": "Byte size of output blocks generated by tasks.",
            "metrics_group": "outputs",
            "map_only": True,
        },
    )
    rows_task_outputs_generated: int = field(
        default=0,
        metadata={
            "description": ("Number of output rows generated by tasks."),
            "metrics_group": "outputs",
            "map_only": True,
        },
    )
    num_outputs_taken: int = field(
        default=0,
        metadata={
            "description": (
                "Number of output blocks that are already "
                "taken by downstream operators."
            ),
            "metrics_group": "outputs",
        },
    )
    bytes_outputs_taken: int = field(
        default=0,
        metadata={
            "description": (
                "Byte size of output blocks that are already "
                "taken by downstream operators."
            ),
            "metrics_group": "outputs",
        },
    )
    num_outputs_of_finished_tasks: int = field(
        default=0,
        metadata={
            "description": (
                "Number of generated output blocks that are from finished tasks."
            ),
            "metrics_group": "outputs",
            "map_only": True,
        },
    )
    bytes_outputs_of_finished_tasks: int = field(
        default=0,
        metadata={
            "description": (
                "Byte size of generated output blocks that are from finished tasks."
            ),
            "metrics_group": "outputs",
            "map_only": True,
        },
    )

    # === Tasks-related metrics ===
    num_tasks_submitted: int = field(
        default=0,
        metadata={
            "description": "Number of submitted tasks.",
            "metrics_group": "tasks",
            "map_only": True,
        },
    )
    num_tasks_running: int = field(
        default=0,
        metadata={
            "description": "Number of running tasks.",
            "metrics_group": "tasks",
            "map_only": True,
        },
    )
    num_tasks_have_outputs: int = field(
        default=0,
        metadata={
            "description": "Number of tasks that already have output.",
            "metrics_group": "tasks",
            "map_only": True,
        },
    )
    num_tasks_finished: int = field(
        default=0,
        metadata={
            "description": "Number of finished tasks.",
            "metrics_group": "tasks",
            "map_only": True,
        },
    )
    num_tasks_failed: int = field(
        default=0,
        metadata={
            "description": "Number of failed tasks.",
            "metrics_group": "tasks",
            "map_only": True,
        },
    )
    block_generation_time: float = field(
        default=0,
        metadata={
            "description": "Time spent generating blocks in tasks.",
            "metrics_group": "tasks",
            "map_only": True,
        },
    )
    task_submission_backpressure_time: float = field(
        default=0,
        metadata={
            "description": "Time spent in task submission backpressure.",
            "metrics_group": "tasks",
        },
    )

    # === Object store memory metrics ===
    obj_store_mem_internal_inqueue_blocks: int = field(
        default=0,
        metadata={
            "description": "Number of blocks in operator's internal input queue.",
            "metrics_group": "object_store_memory",
        },
    )
    obj_store_mem_internal_inqueue: int = field(
        default=0,
        metadata={
            "description": (
                "Byte size of input blocks in the operator's internal input queue."
            ),
            "metrics_group": "object_store_memory",
        },
    )
    obj_store_mem_internal_outqueue_blocks: int = field(
        default=0,
        metadata={
            "description": "Number of blocks in the operator's internal output queue.",
            "metrics_group": "object_store_memory",
        },
    )
    obj_store_mem_internal_outqueue: int = field(
        default=0,
        metadata={
            "description": (
                "Byte size of output blocks in the operator's internal output queue."
            ),
            "metrics_group": "object_store_memory",
        },
    )
    obj_store_mem_pending_task_inputs: int = field(
        default=0,
        metadata={
            "description": "Byte size of input blocks used by pending tasks.",
            "metrics_group": "object_store_memory",
            "map_only": True,
        },
    )
    obj_store_mem_freed: int = field(
        default=0,
        metadata={
            "description": "Byte size of freed memory in object store.",
            "metrics_group": "object_store_memory",
            "map_only": True,
        },
    )
    obj_store_mem_spilled: int = field(
        default=0,
        metadata={
            "description": "Byte size of spilled memory in object store.",
            "metrics_group": "object_store_memory",
            "map_only": True,
        },
    )
    obj_store_mem_used: int = field(
        default=0,
        metadata={
            "description": "Byte size of used memory in object store.",
            "metrics_group": "object_store_memory",
        },
    )

    # === Miscellaneous metrics ===
    # Use "metrics_group: "misc" in the metadata for new metrics in this section.

    def __init__(self, op: "PhysicalOperator"):
        from ray.data._internal.execution.operators.map_operator import MapOperator

        self._op = op
        self._is_map = isinstance(op, MapOperator)
        self._running_tasks: Dict[int, RunningTaskInfo] = {}
        self._extra_metrics: Dict[str, Any] = {}
        # Start time of current pause due to task submission backpressure
        self._task_submission_backpressure_start_time = -1

    @property
    def extra_metrics(self) -> Dict[str, Any]:
        """Return a dict of extra metrics."""
        return self._extra_metrics

    def as_dict(self):
        """Return a dict representation of the metrics."""
        result = []
        for f in fields(self):
            if not self._is_map and f.metadata.get("map_only", False):
                continue
            value = getattr(self, f.name)
            result.append((f.name, value))

        # TODO: record resource usage in OpRuntimeMetrics,
        # avoid calling self._op.current_processor_usage()
        resource_usage = self._op.current_processor_usage()
        result.extend(
            [
                ("cpu_usage", resource_usage.cpu or 0),
                ("gpu_usage", resource_usage.gpu or 0),
            ]
        )
        result.extend(self._extra_metrics.items())
        return dict(result)

    @classmethod
    def get_metric_keys(cls):
        """Return a list of metric keys."""
        return [f.name for f in fields(cls)] + ["cpu_usage", "gpu_usage"]

    @property
    def average_num_outputs_per_task(self) -> Optional[float]:
        """Average number of output blocks per task, or None if no task has finished."""
        if self.num_tasks_finished == 0:
            return None
        else:
            return self.num_outputs_of_finished_tasks / self.num_tasks_finished

    @property
    def average_bytes_per_output(self) -> Optional[float]:
        """Average size in bytes of output blocks."""
        if self.num_task_outputs_generated == 0:
            return None
        else:
            return self.bytes_task_outputs_generated / self.num_task_outputs_generated

    @property
    def obj_store_mem_pending_task_outputs(self) -> Optional[float]:
        """Estimated size in bytes of output blocks in Ray generator buffers.

        If an estimate isn't available, this property returns ``None``.
        """
        per_task_output = self.obj_store_mem_max_pending_output_per_task
        if per_task_output is None:
            return None

        # Ray Data launches multiple tasks per actor, but only one task runs at a
        # time per actor. So, the number of actually running tasks is capped by the
        # number of active actors.
        from ray.data._internal.execution.operators.actor_pool_map_operator import (
            ActorPoolMapOperator,
        )

        num_tasks_running = self.num_tasks_running
        if isinstance(self._op, ActorPoolMapOperator):
            num_tasks_running = min(
                num_tasks_running, self._op._actor_pool.num_active_actors()
            )

        return num_tasks_running * per_task_output

    @property
    def obj_store_mem_max_pending_output_per_task(self) -> Optional[float]:
        """Estimated size in bytes of output blocks in a task's generator buffer."""
        context = ray.data.DataContext.get_current()
        if context._max_num_blocks_in_streaming_gen_buffer is None:
            return None

        bytes_per_output = (
            self.average_bytes_per_output or context.target_max_block_size
        )

        num_pending_outputs = context._max_num_blocks_in_streaming_gen_buffer
        if self.average_num_outputs_per_task is not None:
            num_pending_outputs = min(
                num_pending_outputs, self.average_num_outputs_per_task
            )
        return bytes_per_output * num_pending_outputs

    @property
    def average_bytes_inputs_per_task(self) -> Optional[float]:
        """Average size in bytes of ref bundles passed to tasks, or ``None`` if no
        tasks have been submitted."""
        if self.num_tasks_submitted == 0:
            return None
        else:
            return self.bytes_inputs_of_submitted_tasks / self.num_tasks_submitted

    @property
    def average_bytes_outputs_per_task(self) -> Optional[float]:
        """Average size in bytes of output blocks per task,
        or None if no task has finished."""
        if self.num_tasks_finished == 0:
            return None
        else:
            return self.bytes_outputs_of_finished_tasks / self.num_tasks_finished

    @property
    def average_bytes_change_per_task(self) -> Optional[float]:
        """Average size difference in bytes of input ref bundles and output ref
        bundles per task."""
        if (
            self.average_bytes_inputs_per_task is None
            or self.average_bytes_outputs_per_task is None
        ):
            return None

        return self.average_bytes_outputs_per_task - self.average_bytes_inputs_per_task

    def on_input_received(self, input: RefBundle):
        """Callback when the operator receives a new input."""
        self.num_inputs_received += 1
        self.bytes_inputs_received += input.size_bytes()

    def on_input_queued(self, input: RefBundle):
        """Callback when the operator queues an input."""
        self.obj_store_mem_internal_inqueue_blocks += len(input.blocks)
        self.obj_store_mem_internal_inqueue += input.size_bytes()

    def on_input_dequeued(self, input: RefBundle):
        """Callback when the operator dequeues an input."""
        self.obj_store_mem_internal_inqueue_blocks -= len(input.blocks)
        input_size = input.size_bytes()
        self.obj_store_mem_internal_inqueue -= input_size
        assert self.obj_store_mem_internal_inqueue >= 0, (
            self._op,
            self.obj_store_mem_internal_inqueue,
            input_size,
        )

    def on_output_queued(self, output: RefBundle):
        """Callback when an output is queued by the operator."""
        self.obj_store_mem_internal_outqueue_blocks += len(output.blocks)
        self.obj_store_mem_internal_outqueue += output.size_bytes()

    def on_output_dequeued(self, output: RefBundle):
        """Callback when an output is dequeued by the operator."""
        self.obj_store_mem_internal_outqueue_blocks -= len(output.blocks)
        output_size = output.size_bytes()
        self.obj_store_mem_internal_outqueue -= output_size
        assert self.obj_store_mem_internal_outqueue >= 0, (
            self._op,
            self.obj_store_mem_internal_outqueue,
            output_size,
        )

    def on_toggle_task_submission_backpressure(self, in_backpressure):
        if in_backpressure and self._task_submission_backpressure_start_time == -1:
            # backpressure starting, start timer
            self._task_submission_backpressure_start_time = time.perf_counter()
        elif self._task_submission_backpressure_start_time != -1:
            # backpressure stopping, stop timer
            self.task_submission_backpressure_time += (
                time.perf_counter() - self._task_submission_backpressure_start_time
            )
            self._task_submission_backpressure_start_time = -1

    def on_output_taken(self, output: RefBundle):
        """Callback when an output is taken from the operator."""
        self.num_outputs_taken += 1
        self.bytes_outputs_taken += output.size_bytes()

    def on_task_submitted(self, task_index: int, inputs: RefBundle):
        """Callback when the operator submits a task."""
        self.num_tasks_submitted += 1
        self.num_tasks_running += 1
        self.bytes_inputs_of_submitted_tasks += inputs.size_bytes()
        self.obj_store_mem_pending_task_inputs += inputs.size_bytes()
        self._running_tasks[task_index] = RunningTaskInfo(inputs, 0, 0)

    def on_task_output_generated(self, task_index: int, output: RefBundle):
        """Callback when a new task generates an output."""
        num_outputs = len(output)
        output_bytes = output.size_bytes()

        self.num_task_outputs_generated += num_outputs
        self.bytes_task_outputs_generated += output_bytes

        task_info = self._running_tasks[task_index]
        if task_info.num_outputs == 0:
            self.num_tasks_have_outputs += 1
        task_info.num_outputs += num_outputs
        task_info.bytes_outputs += output_bytes

        for block_ref, meta in output.blocks:
            assert meta.exec_stats and meta.exec_stats.wall_time_s
            self.block_generation_time += meta.exec_stats.wall_time_s
            assert meta.num_rows is not None
            self.rows_task_outputs_generated += meta.num_rows
            trace_allocation(block_ref, "operator_output")

    def on_task_finished(self, task_index: int, exception: Optional[Exception]):
        """Callback when a task is finished."""
        self.num_tasks_running -= 1
        self.num_tasks_finished += 1
        if exception is not None:
            self.num_tasks_failed += 1

        task_info = self._running_tasks[task_index]
        self.num_outputs_of_finished_tasks += task_info.num_outputs
        self.bytes_outputs_of_finished_tasks += task_info.bytes_outputs

        inputs = self._running_tasks[task_index].inputs
        self.num_task_inputs_processed += len(inputs)
        total_input_size = inputs.size_bytes()
        self.bytes_task_inputs_processed += total_input_size
        input_size = inputs.size_bytes()
        self.obj_store_mem_pending_task_inputs -= input_size
        assert self.obj_store_mem_pending_task_inputs >= 0, (
            self._op,
            self.obj_store_mem_pending_task_inputs,
            input_size,
        )

        blocks = [input[0] for input in inputs.blocks]
        metadata = [input[1] for input in inputs.blocks]
        ctx = ray.data.context.DataContext.get_current()
        if ctx.enable_get_object_locations_for_metrics:
            locations = ray.experimental.get_object_locations(blocks)
            for block, meta in zip(blocks, metadata):
                if locations[block].get("did_spill", False):
                    assert meta.size_bytes is not None
                    self.obj_store_mem_spilled += meta.size_bytes

        self.obj_store_mem_freed += total_input_size

        inputs.destroy_if_owned()
        del self._running_tasks[task_index]
