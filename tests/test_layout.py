import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class LayoutTests(unittest.TestCase):
    def test_root_cli_entry_from_another_directory(self):
        with tempfile.TemporaryDirectory() as cwd:
            result = subprocess.run([sys.executable, str(ROOT / "luigi.py"), "--help"],
                                    cwd=cwd, capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("bootstrap", result.stdout)

    @unittest.skipUnless(os.name == "nt", "Windows gate")
    def test_isolated_gate_does_not_import_from_workload_cwd(self):
        with tempfile.TemporaryDirectory() as cwd:
            Path(cwd, "json.py").write_text("raise RuntimeError('untrusted cwd module')\n")
            executable = ROOT / "luigi_app/core/gpufifo.py"
            cmd = [sys.executable, "-I", str(executable), "_gate"]
            # Closing the release gate must not start anything, including on Windows.
            result = subprocess.run(cmd, input=b"", cwd=cwd, capture_output=True, timeout=5)
            self.assertEqual(result.returncode, 125, result.stderr)
            if os.name == "nt":
                import json
                result = subprocess.run(cmd, input=b"R" + json.dumps([sys.executable, "-c",
                    "import sys; sys.exit(41)"]).encode("ascii"), cwd=cwd,
                    capture_output=True, timeout=5)
                self.assertEqual(result.returncode, 41, result.stderr)
