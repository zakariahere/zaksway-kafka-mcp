import asyncio
import os
import time

from functools import lru_cache

from confluent_kafka import (
    Consumer,
    ConsumerGroupTopicPartitions,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewPartitions,
    NewTopic,
    OffsetSpec,
    ResourceType,
)
from mcp.server import FastMCP

mcp  = FastMCP("kafka-zaksway")

def main():
    """"Author: Zakaria BOUAZZA"""
    print("Kafka MCP for you agents!")
    mcp.run(transport="stdio")


def _bootstrap() -> str:
    return os.environ.get("BOOTSTRAP_SERVER", "localhost:9092")


@lru_cache(maxsize=1)
def admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": _bootstrap()})


@lru_cache(maxsize=1)
def producer() -> Producer:
    return Producer({"bootstrap.servers": _bootstrap()})


# --------------------------------------------------------------------------- #
# Topics
# --------------------------------------------------------------------------- #

@mcp.tool("List all non internal topics with their partition number and replication factor")
async def list_topics(withInternal: bool) -> list[dict]:
    """List Kafka topics with partition count and replication factor."""
    def _fetch() -> list[dict]:
        md = admin().list_topics(timeout=10)
        return [
            {
                "name": name,
                "partitions": len(t.partitions),
                "replication-factor": len(next(iter(t.partitions.values())).replicas)
                 if t.partitions else 0
            }
            for name, t in md.topics.items() if withInternal or not name.startswith("__")
        ]
    return await asyncio.to_thread(_fetch)

@mcp.tool("Describe a Kafka topic with its partition layout leader replicas ISR and config overrides")
async def describe_topic(topic: str) -> dict:
    """Describe a single Kafka topic: per-partition leader/replicas/ISR and any non-default config overrides."""
    def _fetch() -> dict:
        client = admin()

        meta = client.list_topics(timeout=10).topics.get(topic)
        if meta is None:
            raise ValueError(f"Topic '{topic}' not found")

        partition_detail = [
            {
                "partition": p.id,
                "leader": p.leader,
                "replicas": list(p.replicas),
                "isr": list(p.isrs),
            }
            for p in sorted(meta.partitions.values(), key=lambda p: p.id)
        ]

        resource = ConfigResource(ResourceType.TOPIC, topic)
        entries = client.describe_configs([resource])[resource].result(timeout=10)
        config = {
            name: entry.value
            for name, entry in entries.items()
            if not entry.is_default
        }

        return {
            "name": topic,
            "internal": topic.startswith("_"),
            "partitions": len(meta.partitions),
            "replication-factor": len(partition_detail[0]["replicas"]) if partition_detail else 0,
            "partition_detail": partition_detail,
            "config": config,
        }

    return await asyncio.to_thread(_fetch)


@mcp.tool("Create a Kafka topic with a partition count replication factor and optional config")
async def create_topic(
    topic: str,
    partitions: int = 1,
    replication_factor: int = 1,
    config: dict[str, str] | None = None,
) -> dict:
    """Create a new Kafka topic. Fails if the topic already exists."""
    def _do() -> dict:
        new_topic = NewTopic(
            topic,
            num_partitions=partitions,
            replication_factor=replication_factor,
            config=config or {},
        )
        admin().create_topics([new_topic], operation_timeout=30)[topic].result()
        return {
            "created": topic,
            "partitions": partitions,
            "replication-factor": replication_factor,
            "config": config or {},
        }
    return await asyncio.to_thread(_do)


@mcp.tool("Delete a Kafka topic permanently and all of its data")
async def delete_topic(topic: str) -> dict:
    """Permanently delete a Kafka topic. This is irreversible."""
    def _do() -> dict:
        admin().delete_topics([topic], operation_timeout=30)[topic].result()
        return {"deleted": topic}
    return await asyncio.to_thread(_do)


@mcp.tool("Increase the partition count of an existing Kafka topic")
async def add_partitions(topic: str, new_total_count: int) -> dict:
    """Grow a topic to new_total_count partitions. Partition counts can only increase, never shrink."""
    def _do() -> dict:
        admin().create_partitions([NewPartitions(topic, new_total_count)])[topic].result()
        return {"topic": topic, "new_total_partitions": new_total_count}
    return await asyncio.to_thread(_do)


@mcp.tool("Set or update configuration entries on a Kafka topic")
async def alter_topic_config(topic: str, config: dict[str, str]) -> dict:
    """Incrementally SET the given config keys on a topic, leaving other settings untouched."""
    def _do() -> dict:
        entries = [
            ConfigEntry(key, value, incremental_operation=AlterConfigOpType.SET)
            for key, value in config.items()
        ]
        resource = ConfigResource(ResourceType.TOPIC, topic, incremental_configs=entries)
        admin().incremental_alter_configs([resource])[resource].result()
        return {"topic": topic, "updated": config}
    return await asyncio.to_thread(_do)


@mcp.tool("Get earliest and latest offsets watermarks per partition for a Kafka topic")
async def get_topic_offsets(topic: str) -> dict:
    """Return the earliest and latest offset for each partition, plus the message count between them."""
    def _do() -> dict:
        client = admin()
        meta = client.list_topics(timeout=10).topics.get(topic)
        if meta is None:
            raise ValueError(f"Topic '{topic}' not found")

        parts = list(meta.partitions.keys())
        earliest_fut = client.list_offsets(
            {TopicPartition(topic, p): OffsetSpec.earliest() for p in parts},
            request_timeout=30,
        )
        latest_fut = client.list_offsets(
            {TopicPartition(topic, p): OffsetSpec.latest() for p in parts},
            request_timeout=30,
        )
        earliest = {tp.partition: f.result().offset for tp, f in earliest_fut.items()}
        latest = {tp.partition: f.result().offset for tp, f in latest_fut.items()}

        rows = [
            {
                "partition": p,
                "earliest": earliest[p],
                "latest": latest[p],
                "message_count": latest[p] - earliest[p],
            }
            for p in sorted(parts)
        ]
        return {
            "topic": topic,
            "total_messages": sum(r["message_count"] for r in rows),
            "partitions": rows,
        }
    return await asyncio.to_thread(_do)


# --------------------------------------------------------------------------- #
# Cluster
# --------------------------------------------------------------------------- #

@mcp.tool("Describe the Kafka cluster brokers controller id and cluster id")
async def describe_cluster() -> dict:
    """Return the cluster id, controller broker id, and the list of brokers."""
    def _do() -> dict:
        md = admin().list_topics(timeout=10)
        brokers = sorted(
            ({"id": b.id, "host": b.host, "port": b.port} for b in md.brokers.values()),
            key=lambda b: b["id"],
        )
        return {
            "cluster_id": md.cluster_id,
            "controller_id": md.controller_id,
            "broker_count": len(brokers),
            "brokers": brokers,
        }
    return await asyncio.to_thread(_do)


# --------------------------------------------------------------------------- #
# Consumer groups
# --------------------------------------------------------------------------- #

@mcp.tool("List all Kafka consumer groups with their state")
async def list_consumer_groups() -> list[dict]:
    """List every consumer group known to the cluster with its current state."""
    def _do() -> list[dict]:
        result = admin().list_consumer_groups(request_timeout=10).result()
        return [
            {
                "group_id": g.group_id,
                "is_simple": g.is_simple_consumer_group,
                "state": g.state.name,
            }
            for g in result.valid
        ]
    return await asyncio.to_thread(_do)


@mcp.tool("Describe a Kafka consumer group state coordinator members and assignments")
async def describe_consumer_group(group_id: str) -> dict:
    """Describe one consumer group: state, coordinator, and each member's partition assignment."""
    def _do() -> dict:
        g = admin().describe_consumer_groups([group_id], request_timeout=10)[group_id].result()
        members = [
            {
                "member_id": m.member_id,
                "client_id": m.client_id,
                "host": m.host,
                "assignment": [
                    {"topic": tp.topic, "partition": tp.partition}
                    for tp in (m.assignment.topic_partitions if m.assignment else [])
                ],
            }
            for m in g.members
        ]
        coord = g.coordinator
        return {
            "group_id": g.group_id,
            "state": g.state.name,
            "partition_assignor": g.partition_assignor,
            "coordinator": {"id": coord.id, "host": coord.host, "port": coord.port} if coord else None,
            "members": members,
        }
    return await asyncio.to_thread(_do)


@mcp.tool("Show committed offsets and lag for a Kafka consumer group")
async def consumer_group_lag(group_id: str) -> dict:
    """Join the group's committed offsets with each partition's end offset to compute lag."""
    def _do() -> dict:
        client = admin()

        committed = client.list_consumer_group_offsets(
            [ConsumerGroupTopicPartitions(group_id)]
        )[group_id].result().topic_partitions
        if not committed:
            return {"group_id": group_id, "total_lag": 0, "partitions": []}

        end_fut = client.list_offsets(
            {TopicPartition(tp.topic, tp.partition): OffsetSpec.latest() for tp in committed},
            request_timeout=30,
        )
        end_offsets = {(tp.topic, tp.partition): f.result().offset for tp, f in end_fut.items()}

        rows = []
        for tp in committed:
            end = end_offsets[(tp.topic, tp.partition)]
            lag = end - tp.offset if tp.offset >= 0 else None
            rows.append({
                "topic": tp.topic,
                "partition": tp.partition,
                "committed_offset": tp.offset,
                "end_offset": end,
                "lag": lag,
            })
        rows.sort(key=lambda r: (r["topic"], r["partition"]))
        return {
            "group_id": group_id,
            "total_lag": sum(r["lag"] for r in rows if r["lag"] is not None),
            "partitions": rows,
        }
    return await asyncio.to_thread(_do)


@mcp.tool("Delete a Kafka consumer group permanently")
async def delete_consumer_group(group_id: str) -> dict:
    """Permanently delete a consumer group. The group must have no active members."""
    def _do() -> dict:
        admin().delete_consumer_groups([group_id], request_timeout=10)[group_id].result()
        return {"deleted": group_id}
    return await asyncio.to_thread(_do)


# --------------------------------------------------------------------------- #
# Data plane (produce / consume)
# --------------------------------------------------------------------------- #

@mcp.tool("Produce a single message to a Kafka topic")
async def produce_message(
    topic: str,
    value: str,
    key: str | None = None,
    partition: int | None = None,
) -> dict:
    """Produce one message and wait for the broker delivery acknowledgement."""
    def _do() -> dict:
        report: dict = {}

        def _on_delivery(err, msg):
            if err is not None:
                report["error"] = str(err)
            else:
                report["partition"] = msg.partition()
                report["offset"] = msg.offset()

        kwargs: dict = {"value": value.encode("utf-8"), "callback": _on_delivery}
        if key is not None:
            kwargs["key"] = key.encode("utf-8")
        if partition is not None:
            kwargs["partition"] = partition

        p = producer()
        p.produce(topic, **kwargs)
        p.flush(10)

        if "error" in report:
            raise RuntimeError(f"Delivery failed: {report['error']}")
        return {"delivered": True, "topic": topic, **report}
    return await asyncio.to_thread(_do)


@mcp.tool("Peek recent messages from a Kafka topic without committing offsets")
async def consume_messages(
    topic: str,
    max_messages: int = 10,
    timeout_seconds: float = 5.0,
    from_beginning: bool = True,
) -> dict:
    """Read up to max_messages from a topic via manual assignment, leaving no committed offsets."""
    def _do() -> dict:
        consumer = Consumer({
            "bootstrap.servers": _bootstrap(),
            "group.id": "kafka-mcp-peek",
            "enable.auto.commit": False,
        })
        messages: list[dict] = []
        try:
            meta = consumer.list_topics(topic, timeout=10).topics.get(topic)
            if meta is None:
                raise ValueError(f"Topic '{topic}' not found")

            assignments = []
            for p in meta.partitions:
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
                start = low if from_beginning else max(low, high - max_messages)
                assignments.append(TopicPartition(topic, p, start))
            consumer.assign(assignments)

            deadline = time.monotonic() + timeout_seconds
            while len(messages) < max_messages and time.monotonic() < deadline:
                msg = consumer.poll(timeout=0.5)
                if msg is None or msg.error():
                    continue
                key = msg.key()
                value = msg.value()
                messages.append({
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "key": key.decode("utf-8", "replace") if key is not None else None,
                    "value": value.decode("utf-8", "replace") if value is not None else None,
                    "timestamp": msg.timestamp()[1],
                })
        finally:
            consumer.close()
        return {"topic": topic, "count": len(messages), "messages": messages}
    return await asyncio.to_thread(_do)
