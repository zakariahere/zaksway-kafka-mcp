"""Backward-compatible entry point.

The server now lives in the installable package `src/zaksway_kafka_mcp`.
This shim keeps `uv run main.py` working for existing MCP client configs;
the preferred entry points are the `kafka-zaksway` console script and
`python -m zaksway_kafka_mcp`.
"""
from zaksway_kafka_mcp import main

if __name__ == "__main__":
    main()
