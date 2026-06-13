# 🦊 kafka-mcp

> An [MCP](https://modelcontextprotocol.io) server that gives your AI agents eyes into Apache Kafka.

`kafka-mcp` exposes Kafka administration operations as **Model Context Protocol tools**, so assistants like
**Claude Desktop**, **Claude Code**, or any MCP-compatible client can inspect and operate your cluster in plain language —
*"list all my topics with their replication factor"* — instead of you reaching for the CLI.

It ships with a batteries-included `docker-compose.yml` that spins up a complete local Kafka lab
(KRaft broker + Schema Registry + Web UI) so you can try it end-to-end in minutes.

<p align="left">
  <img alt="Python" src="https://img.shields.io/badge/python-3.12+-3776AB?logo=python&logoColor=white">
  <img alt="MCP" src="https://img.shields.io/badge/protocol-MCP-6E56CF">
  <img alt="Apache Kafka" src="https://img.shields.io/badge/Apache-Kafka-231F20?logo=apachekafka&logoColor=white">
  <img alt="uv" src="https://img.shields.io/badge/built%20with-uv-DE5FE9">
</p>

---

## ✨ Features

- 🔌 **Drop-in MCP server** — runs over `stdio`, so any MCP client can launch it as a subprocess.
- 📋 **Topic management** — list & describe topics, create / delete, add partitions, and read or alter configs.
- 👥 **Consumer group insight** — list & describe groups, inspect members & assignments, and compute per-partition lag.
- 📨 **Produce & peek** — send a message to a topic, or read recent records back without committing offsets.
- 🩺 **Cluster & offset views** — describe brokers / controller and fetch earliest / latest watermarks per partition.
- ⚡ **Async-friendly** — blocking Kafka admin calls are offloaded to worker threads so the event loop stays snappy.
- 🐳 **Self-contained local lab** — one `docker compose up` gives you Kafka (KRaft, no ZooKeeper), Schema Registry, and a Web UI.
- 🛠️ **Tiny & hackable** — a single `main.py` you can read in one sitting and extend with new tools.

---

## 🧭 How it works

```
┌──────────────────┐   MCP over stdio   ┌──────────────────┐   Kafka Admin API   ┌──────────────────┐
│   AI Agent       │ ◀───────────────▶  │   kafka-mcp      │ ◀────────────────▶  │   Kafka broker   │
│ (Claude, etc.)   │   tool calls       │  (FastMCP server)│   confluent-kafka   │  (localhost:9092)│
└──────────────────┘                    └──────────────────┘                     └──────────────────┘
```

The agent never talks to Kafka directly — it calls a **tool**, `kafka-mcp` translates that into a
`confluent-kafka` admin or client request, and returns structured JSON the model can reason about.

---

## 📦 Prerequisites

- **Python 3.12+**
- **[uv](https://docs.astral.sh/uv/)** for dependency management (recommended)
- **Docker** + **Docker Compose** (only if you want the local Kafka lab)

---

## 🚀 Quick start

### 1. Clone & install

```bash
git clone <your-repo-url> kafka-mcp
cd kafka-mcp
uv sync
```

### 2. Start a local Kafka (optional, but handy)

```bash
docker compose up -d
```

This brings up three services:

| Service             | URL / Port              | What it's for                                  |
| ------------------- | ----------------------- | ---------------------------------------------- |
| **Kafka broker**    | `localhost:9092`        | The broker your MCP server connects to         |
| **Schema Registry** | `http://localhost:8081` | Avro/Protobuf/JSON schema management           |
| **Kafka UI**        | `http://localhost:8080` | Browse topics, messages, and consumer groups   |

> 💡 Auto-create topics is enabled, so you can produce to a new topic and watch it appear via the MCP `list_topics` tool.

### 3. Run the MCP server

```bash
uv run main.py
```

You should see:

```
Kafka MCP for you agents!
```

The server is now listening on `stdio`, ready for an MCP client to connect.

---

## 🤖 Connecting an MCP client

Most clients (Claude Desktop, Claude Code, …) launch MCP servers from a JSON config.
Point them at this project like so:

```jsonc
{
  "mcpServers": {
    "kafka-zaksway": {
      "command": "uv",
      "args": ["--directory", "/absolute/path/to/kafka-mcp", "run", "main.py"],
      "env": {
        "BOOTSTRAP_SERVER": "localhost:9092"
      }
    }
  }
}
```

- **Claude Desktop** → add the block to `claude_desktop_config.json`.
- **Claude Code** → `claude mcp add kafka-zaksway -- uv --directory /absolute/path/to/kafka-mcp run main.py`

Restart the client, and `kafka-zaksway` will appear among your available tools.

---

## 🧰 Available tools

`kafka-mcp` exposes **14 tools** spanning topic management, consumer groups, the cluster, and the data plane.
Tools marked ⚠️ are destructive (they delete data) — agents should confirm before calling them.

| Category | Tool | Parameters | Description |
| -------- | ---- | ---------- | ----------- |
| **Topics** | `list_topics` | `withInternal: bool` | List topics with partition count & replication factor. |
| **Topics** | `describe_topic` | `topic: str` | Per-partition leader / replicas / ISR + non-default config overrides. |
| **Topics** | `create_topic` | `topic: str`, `partitions: int = 1`, `replication_factor: int = 1`, `config: dict = {}` | Create a new topic. |
| **Topics** | `delete_topic` ⚠️ | `topic: str` | Permanently delete a topic and all of its data. |
| **Topics** | `add_partitions` | `topic: str`, `new_total_count: int` | Increase a topic's partition count (cannot shrink). |
| **Topics** | `alter_topic_config` | `topic: str`, `config: dict` | Set / update topic configuration entries. |
| **Topics** | `get_topic_offsets` | `topic: str` | Earliest & latest offsets (watermarks) per partition. |
| **Cluster** | `describe_cluster` | — | Cluster id, controller broker, and broker list. |
| **Groups** | `list_consumer_groups` | — | All consumer groups with their state. |
| **Groups** | `describe_consumer_group` | `group_id: str` | State, coordinator, members & their partition assignments. |
| **Groups** | `consumer_group_lag` | `group_id: str` | Committed offset, end offset, and lag per partition. |
| **Groups** | `delete_consumer_group` ⚠️ | `group_id: str` | Permanently delete a consumer group. |
| **Data** | `produce_message` | `topic: str`, `value: str`, `key: str = null`, `partition: int = null` | Produce a single message and await delivery. |
| **Data** | `consume_messages` | `topic: str`, `max_messages: int = 10`, `timeout_seconds: float = 5.0`, `from_beginning: bool = true` | Peek recent messages without committing offsets. |

> 💡 The registered MCP tool names are full descriptive sentences (e.g. `Show committed offsets and lag for a Kafka consumer group`); the short identifiers above mirror the Python functions in `main.py` and are used here for brevity.

**Example — `list_topics` response:**

```json
[
  { "name": "orders",   "partitions": 6, "replication-factor": 1 },
  { "name": "payments", "partitions": 3, "replication-factor": 1 }
]
```

---

## ⚙️ Configuration

The server is configured entirely through environment variables.

| Variable           | Default          | Description                              |
| ------------------ | ---------------- | ---------------------------------------- |
| `BOOTSTRAP_SERVER` | `localhost:9092` | Kafka bootstrap server(s) to connect to. |

---

## 📦 Releasing to PyPI

The package is published to PyPI as **[`zaksway-kafka-mcp`](https://pypi.org/p/zaksway-kafka-mcp)** by a GitHub Actions workflow (`.github/workflows/publish.yml`) that triggers on `v*` version tags and authenticates via **Trusted Publishing (OIDC)** — no API tokens stored anywhere.

**One-time setup** — register a [Trusted Publisher](https://docs.pypi.org/trusted-publishers/) on PyPI:

| Field             | Value                  |
| ----------------- | ---------------------- |
| Owner             | `zakariahere`          |
| Repository        | `zaksway-kafka-mcp`    |
| Workflow filename | `publish.yml`          |
| Environment       | `pypi`                 |

**To cut a release:**

```bash
# 1. Bump `version` in pyproject.toml (e.g. 0.1.0 -> 0.2.0), then:
git commit -am "release: v0.2.0"
git tag v0.2.0
git push origin master --tags
```

The workflow verifies the tag matches `pyproject.toml`, builds the wheel + sdist, smoke-tests both, and publishes. **Once published**, anyone can run it with zero install:

```bash
uvx zaksway-kafka-mcp           # run the server directly
# or
pip install zaksway-kafka-mcp   # then run: kafka-zaksway
```

---

## 🗂️ Project structure

```
kafka-mcp/
├── src/zaksway_kafka_mcp/
│   ├── __init__.py     # The MCP server + all 14 tool definitions
│   └── __main__.py     # `python -m zaksway_kafka_mcp` entry point
├── tests/
│   └── smoke_test.py   # Import/packaging check run in CI before publish
├── .github/workflows/
│   └── publish.yml     # Build + publish to PyPI on `v*` tags (Trusted Publishing)
├── main.py             # Backward-compat shim (keeps `uv run main.py` working)
├── docker-compose.yml  # Local Kafka lab (broker + schema registry + UI)
├── pyproject.toml      # Project metadata, dependencies & build backend
├── uv.lock             # Pinned dependency lockfile
└── README.md           # You are here
```

---

## 🛣️ Roadmap

Recently shipped ✅

- [x] `create_topic` / `delete_topic`
- [x] `add_partitions` & `alter_topic_config`
- [x] Describe consumer groups & their lag
- [x] Peek at the latest messages on a topic

Ideas for what's next:

- [ ] Reset / set consumer group offsets
- [ ] ACL management (list / create / delete)
- [ ] Broker config inspection
- [ ] Schema Registry integration (list subjects & schemas)

---

## 🧑‍💻 Author

**Zakaria BOUAZZA** :
https://zakaria.lu
---

## 📄 License

No license has been specified yet. Add one (e.g. [MIT](https://choosealicense.com/licenses/mit/)) before sharing publicly.
