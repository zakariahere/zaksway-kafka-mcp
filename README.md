# 🦊 kafka-mcp

> An [MCP](https://modelcontextprotocol.io) server that gives your AI agents eyes into Apache Kafka.

`kafka-mcp` exposes Kafka administration operations as **Model Context Protocol tools**, so assistants like
**Claude Desktop**, **Claude Code**, or any MCP-compatible client can inspect your cluster in plain language —
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
- 📋 **Topic introspection** — list topics with their partition count and replication factor, with an option to include or hide internal (`__`) topics.
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
`confluent-kafka` admin request, and returns structured JSON the model can reason about.

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

### `list_topics`

List Kafka topics with their partition count and replication factor.

| Parameter      | Type   | Description                                                              |
| -------------- | ------ | ----------------------------------------------------------------------- |
| `withInternal` | `bool` | When `true`, include internal topics (those starting with `__`).        |

**Example response:**

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

## 🗂️ Project structure

```
kafka-mcp/
├── main.py             # The MCP server + tool definitions
├── docker-compose.yml  # Local Kafka lab (broker + schema registry + UI)
├── pyproject.toml      # Project metadata & dependencies
├── uv.lock             # Pinned dependency lockfile
└── README.md           # You are here
```

---

## 🛣️ Roadmap

A few natural next tools to add:

- [ ] `create_topic` / `delete_topic`
- [ ] Describe consumer groups & their lag
- [ ] Read/alter topic configs
- [ ] Peek at the latest messages on a topic

---

## 🧑‍💻 Author

**Zakaria BOUAZZA** :
https://zakaria.lu
---

## 📄 License

No license has been specified yet. Add one (e.g. [MIT](https://choosealicense.com/licenses/mit/)) before sharing publicly.
