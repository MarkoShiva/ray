import ray
import ray._private.profiling as profiling
import ray._private.services as services
import ray._private.utils as utils
import ray._private.worker
from ray._private import ray_constants
from ray._private.state import GlobalState
from ray._raylet import GcsClientOptions

__all__ = ["free", "global_gc"]
MAX_MESSAGE_LENGTH = ray._config.max_grpc_message_size()


def global_gc():
    """Trigger gc.collect() on all workers in the cluster."""

    worker = ray._private.worker.global_worker
    worker.core_worker.global_gc()


def memory_summary(
    address=None,
    redis_password=ray_constants.REDIS_DEFAULT_PASSWORD,
    group_by="NODE_ADDRESS",
    sort_by="OBJECT_SIZE",
    units="B",
    line_wrap=True,
    stats_only=False,
    num_entries=None,
):
    from ray.dashboard.memory_utils import memory_summary

    address = services.canonicalize_bootstrap_address_or_die(address)

    state = GlobalState()
    options = GcsClientOptions.from_gcs_address(address)
    state._initialize_global_state(options)
    if stats_only:
        return get_store_stats(state)
    return memory_summary(
        state, group_by, sort_by, line_wrap, units, num_entries
    ) + get_store_stats(state)


def get_store_stats(state, node_manager_address=None, node_manager_port=None):
    """Returns a formatted string describing memory usage in the cluster."""

    from ray.core.generated import node_manager_pb2, node_manager_pb2_grpc

    # We can ask any Raylet for the global memory info, that Raylet internally
    # asks all nodes in the cluster for memory stats.
    if node_manager_address is None or node_manager_port is None:
        raylet = next((node for node in state.node_table() if node["Alive"]), None)
        assert raylet is not None, "Every raylet is dead"
        raylet_address = f'{raylet["NodeManagerAddress"]}:{raylet["NodeManagerPort"]}'
    else:
        raylet_address = f"{node_manager_address}:{node_manager_port}"

    channel = utils.init_grpc_channel(
        raylet_address,
        options=[
            ("grpc.max_send_message_length", MAX_MESSAGE_LENGTH),
            ("grpc.max_receive_message_length", MAX_MESSAGE_LENGTH),
        ],
    )

    stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
    reply = stub.FormatGlobalMemoryInfo(
        node_manager_pb2.FormatGlobalMemoryInfoRequest(include_memory_info=False),
        timeout=60.0,
    )
    return store_stats_summary(reply)


def node_stats(
    node_manager_address=None, node_manager_port=None, include_memory_info=True
):
    """Returns NodeStats object describing memory usage in the cluster."""

    from ray.core.generated import node_manager_pb2, node_manager_pb2_grpc

    # We can ask any Raylet for the global memory info.
    assert node_manager_address is not None and node_manager_port is not None
    raylet_address = f"{node_manager_address}:{node_manager_port}"
    channel = utils.init_grpc_channel(
        raylet_address,
        options=[
            ("grpc.max_send_message_length", MAX_MESSAGE_LENGTH),
            ("grpc.max_receive_message_length", MAX_MESSAGE_LENGTH),
        ],
    )

    stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
    return stub.GetNodeStats(
        node_manager_pb2.GetNodeStatsRequest(
            include_memory_info=include_memory_info
        ),
        timeout=30.0,
    )


def store_stats_summary(reply):
    """Returns formatted string describing object store stats in all nodes."""
    store_summary = (
        "--- Aggregate object store stats across all nodes ---\n"
        + f"Plasma memory usage {int(reply.store_stats.object_store_bytes_used / (1024 * 1024))} MiB, {reply.store_stats.num_local_objects} objects, {round(100 * reply.store_stats.object_store_bytes_used / reply.store_stats.object_store_bytes_avail, 2)}% full, {round(100 * reply.store_stats.object_store_bytes_primary_copy / reply.store_stats.object_store_bytes_avail, 2)}% needed\n"
    )

    if reply.store_stats.object_store_bytes_fallback > 0:
        store_summary += f"Plasma filesystem mmap usage: {int(reply.store_stats.object_store_bytes_fallback / (1024 * 1024))} MiB\n"

    if reply.store_stats.spill_time_total_s > 0:
        store_summary += f"Spilled {int(reply.store_stats.spilled_bytes_total / (1024 * 1024))} MiB, {reply.store_stats.spilled_objects_total} objects, avg write throughput {int(reply.store_stats.spilled_bytes_total / (1024 * 1024) / reply.store_stats.spill_time_total_s)} MiB/s\n"

    if reply.store_stats.restore_time_total_s > 0:
        store_summary += f"Restored {int(reply.store_stats.restored_bytes_total / (1024 * 1024))} MiB, {reply.store_stats.restored_objects_total} objects, avg read throughput {int(reply.store_stats.restored_bytes_total / (1024 * 1024) / reply.store_stats.restore_time_total_s)} MiB/s\n"

    if reply.store_stats.consumed_bytes > 0:
        store_summary += f"Objects consumed by Ray tasks: {int(reply.store_stats.consumed_bytes / (1024 * 1024))} MiB.\n"

    if reply.store_stats.object_pulls_queued:
        store_summary += "Object fetches queued, waiting for available memory."

    return store_summary


def free(object_refs: list, local_only: bool = False):
    """Free a list of IDs from the in-process and plasma object stores.

    This function is a low-level API which should be used in restricted
    scenarios.

    If local_only is false, the request will be send to all object stores.

    This method will not return any value to indicate whether the deletion is
    successful or not. This function is an instruction to the object store. If
    some of the objects are in use, the object stores will delete them later
    when the ref count is down to 0.

    Examples:
        >>> x_id = f.remote()
        >>> ray.get(x_id)  # wait for x to be created first
        >>> free([x_id])  # unpin & delete x globally

    Args:
        object_refs (List[ObjectRef]): List of object refs to delete.
        local_only: Whether only deleting the list of objects in local
            object store or all object stores.
    """
    worker = ray._private.worker.global_worker

    if isinstance(object_refs, ray.ObjectRef):
        object_refs = [object_refs]

    if not isinstance(object_refs, list):
        raise TypeError(f"free() expects a list of ObjectRef, got {type(object_refs)}")

    # Make sure that the values are object refs.
    for object_ref in object_refs:
        if not isinstance(object_ref, ray.ObjectRef):
            raise TypeError(
                f"Attempting to call `free` on the value {object_ref}, which is not an ray.ObjectRef."
            )


    worker.check_connected()
    with profiling.profile("ray.free"):
        if not object_refs:
            return

        worker.core_worker.free_objects(object_refs, local_only)
