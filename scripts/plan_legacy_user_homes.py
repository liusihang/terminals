"""Plan a Docker migration from offline snapshots. Never copy files or call services."""
import argparse
from collections import defaultdict
import hashlib
import json
import os
from pathlib import Path
import sqlite3


def plan(db_path, homes, verified_users, policy):
    """Only map default personal homes with explicit evidence of ownership and policy."""
    if not db_path.is_file() or not homes.is_dir():
        raise ValueError("Provide an offline SQLite snapshot and a separate home snapshot")
    if any(Path(str(db_path) + suffix).exists() for suffix in ("-wal", "-journal")):
        raise ValueError("Use a self-contained SQLite backup (sqlite3 .backup), not a live database")
    with sqlite3.connect(db_path.resolve().as_uri() + "?mode=ro&immutable=1", uri=True) as db:
        record = db.execute("SELECT data FROM policies WHERE id = ?", (policy,)).fetchone()
        if not record:
            raise ValueError("Selected policy is missing")
        fields = json.loads(record[0])
        if any(fields.get(k) for k in ("per_chat", "per_automation", "max_instances", "scope")):
            raise ValueError("Policy requires manual scope or allocation review")
        states = defaultdict(set)
        for user, p, context in db.execute("SELECT user_id,policy_id,context_id FROM policy_lifecycle_states"):
            states[user].add((p, context or "default"))
    entries = {p.name: p for p in homes.iterdir()}
    rows = []
    for user in sorted(entries.keys() | states.keys()):
        entry = entries.get(user)
        reason = None
        if entry is None:
            reason = "missing home"
        elif entry.is_symlink() or not entry.is_dir():
            reason = "not a physical home directory"
        elif user not in verified_users:
            reason = "not on verified WebUI user list"
        elif states[user] != {(policy, "default")}:
            reason = "missing or ambiguous legacy context/policy"
        elif (entry / "contexts").exists() or (entry / "contexts").is_symlink():
            reason = "nested legacy contexts require separate review"
        encoded = json.dumps([policy, "user:" + user], separators=(",", ":"), ensure_ascii=True)
        rows.append({"user_id": user, "review": reason} if reason else {
            "user_id": user, "source": user,
            "destination": ".terminals/" + hashlib.sha256(encoded.encode()).hexdigest()[:32],
        })
    return {"policy": policy, "scope": "user", "homes": rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-snapshot", type=Path, required=True)
    parser.add_argument("--homes-snapshot", type=Path, required=True)
    parser.add_argument("--verified-users", type=Path, required=True, help="JSON list of authorized WebUI user IDs")
    parser.add_argument("--policy", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    users = json.loads(args.verified_users.read_text())
    if not isinstance(users, list) or not all(isinstance(user, str) and user for user in users):
        parser.error("--verified-users must contain a JSON list of nonempty strings")
    report = plan(args.db_snapshot, args.homes_snapshot, set(users), args.policy)
    with os.fdopen(os.open(args.out, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as handle:
        json.dump(report, handle, ensure_ascii=True, indent=2)
        handle.write("\n")
    print("Mapped:", sum("destination" in row for row in report["homes"]), "Review:",
          sum("review" in row for row in report["homes"]))


if __name__ == "__main__":
    main()
