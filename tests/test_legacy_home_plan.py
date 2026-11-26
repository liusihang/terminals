import sqlite3
import tempfile
import unittest
from pathlib import Path

from machines.utils.context import get_terminal_id
from scripts.plan_legacy_user_homes import plan


class MigrationPlanTests(unittest.TestCase):
    def test_only_map_confirmed_default_homes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = root / "snapshot.db"
            homes = root / "homes"
            homes.mkdir()
            with sqlite3.connect(db) as c:
                c.execute("CREATE TABLE policies (id TEXT, data TEXT)")
                c.execute("INSERT INTO policies VALUES('office', '{}')")
                c.execute("CREATE TABLE policy_lifecycle_states (user_id TEXT, policy_id TEXT, context_id TEXT)")
                c.executemany("INSERT INTO policy_lifecycle_states VALUES(?, 'office', 'default')",
                              [(x,) for x in ("alice", "missing", "nested", "system")])
            for user in ("alice", "orphan", "nested", "system"):
                (homes / user).mkdir()
            (homes / "nested" / "contexts").mkdir()
            report = plan(db, homes, {"alice", "missing", "nested", "orphan"}, "office")
            mapped = [(r["user_id"], r["destination"]) for r in report["homes"] if "destination" in r]
            self.assertEqual(mapped, [("alice", ".terminals/" + get_terminal_id("office", "user:alice"))])
            self.assertEqual(sum("review" in r for r in report["homes"]), 4)
