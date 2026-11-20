import contextlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import luigi_app.dashboard.server as dashboard
import luigi_app.cli as luigi
from luigi_app.core.broker import Broker

ADMIN = "synthetic-dashboard-admin-only"
PRIVATE_LINK = "http://127.0.0.1:42001/#synthetic-view-capability"


class Terminal(io.StringIO):
    def isatty(self):
        return True


class DashboardCliTests(unittest.TestCase):
    def test_prompt_once_and_allow_only_read_snapshot(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            broker = Broker(root)
            broker.auth.bootstrap_admin(ADMIN)
            seen = []
            output = Terminal()

            def exchange(current, raw):
                self.assertEqual(current, root)
                request = luigi.decode(raw)
                self.assertEqual(request, {"op": "snapshot", "data": {}, "credential": ADMIN})
                seen.append(request["op"])
                return broker.handle(raw)

            class Session:
                url = PRIVATE_LINK
                closed = False
                def close(self):
                    self.closed = True

            session = Session()
            def opening(provider):
                snap = provider()
                self.assertEqual(snap["queues"], [])
                self.assertNotIn(ADMIN, repr(snap))
                return session

            with contextlib.redirect_stdout(output), patch("luigi_app.cli.read_tty", return_value=ADMIN) as prompt, \
                    patch("luigi_app.cli.transport.client", side_effect=exchange), \
                    patch("luigi_app.dashboard.server.open_dashboard", side_effect=opening), patch("builtins.input", return_value=""):
                self.assertEqual(luigi.run_dashboard(root), 0)
                prompt.assert_called_once()
            self.assertEqual(seen, ["snapshot", "snapshot"])
            self.assertTrue(session.closed)
            self.assertIn(PRIVATE_LINK, output.getvalue())
            self.assertNotIn(ADMIN, output.getvalue())

    def test_no_link_for_non_terminal_or_failed_authentication(self):
        with contextlib.redirect_stdout(io.StringIO()), patch("luigi_app.cli.read_tty") as prompt:
            with self.assertRaisesRegex(ValueError, "local terminal"):
                luigi.run_dashboard(Path("."))
            prompt.assert_not_called()

        with contextlib.redirect_stdout(Terminal()), patch("luigi_app.cli.read_tty", return_value=ADMIN), \
                patch("luigi_app.cli.request", side_effect=ValueError("not authorized")), \
                patch("luigi_app.dashboard.server.open_dashboard") as opening:
            with self.assertRaisesRegex(ValueError, "not authorized"):
                luigi.run_dashboard(Path("."))
            opening.assert_not_called()


if __name__ == "__main__":
    unittest.main()
