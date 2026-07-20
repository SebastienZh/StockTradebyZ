import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "agent"))

from evidence import load_fundamental_evidence


class EvidenceTests(unittest.TestCase):
    def test_future_fundamental_snapshot_is_blocked(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "data" / "fundamentals" / "CN" / "600519.json"
            target.parent.mkdir(parents=True)
            target.write_text(
                json.dumps({"published_at": "2026-04-30", "roe": 0.2}), encoding="utf-8"
            )
            result = load_fundamental_evidence(root, "CN", "600519", "2026-03-31")
            self.assertEqual(result["status"], "stale")


if __name__ == "__main__":
    unittest.main()
