"""PreToolUse hook: force a confirmation prompt for destructive Kafka MCP tools.

Reads the tool-call JSON from stdin and emits a PreToolUse decision of "ask",
which makes Claude Code prompt the user even if the tool is otherwise allowed.
"""
import json
import sys


def main() -> None:
    try:
        data = json.load(sys.stdin)
    except Exception:
        data = {}

    tool_name = data.get("tool_name", "unknown tool")
    tool_input = data.get("tool_input") or {}
    # delete_topic uses `topic`; delete_consumer_group uses `group_id`.
    target = tool_input.get("topic") or tool_input.get("group_id") or "?"
    short = tool_name.split("__")[-1] if "__" in tool_name else tool_name

    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "ask",
            "permissionDecisionReason": (
                f"⚠️ Destructive Kafka operation: {short} targeting "
                f"'{target}'. Confirm before this is executed."
            ),
        }
    }))


if __name__ == "__main__":
    main()
