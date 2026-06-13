"""Smoke test run against the built wheel/sdist in CI.

Confirms the distribution installs, imports, and exposes its entry point and
tools — i.e. we didn't forget to include a crucial file. It does NOT touch a
real broker (all Kafka clients in the package are created lazily).
"""
import zaksway_kafka_mcp as pkg

assert callable(pkg.main), "console entry point main() is missing"

EXPECTED_TOOLS = [
    "list_topics",
    "describe_topic",
    "create_topic",
    "delete_topic",
    "add_partitions",
    "alter_topic_config",
    "get_topic_offsets",
    "describe_cluster",
    "list_consumer_groups",
    "describe_consumer_group",
    "consumer_group_lag",
    "delete_consumer_group",
    "produce_message",
    "consume_messages",
]
missing = [name for name in EXPECTED_TOOLS if not hasattr(pkg, name)]
assert not missing, f"missing tools in distribution: {missing}"

print(f"smoke test passed: imported {pkg.__name__} with {len(EXPECTED_TOOLS)} tools + main()")
