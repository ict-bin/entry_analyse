import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app import runner


class RunAgentPromptFileTests(unittest.TestCase):
    def test_run_agent_uses_prompt_file_instead_of_raw_argv(self):
        captured = {}

        async def fake_run_with_pi_retry(**kwargs):
            captured["args"] = kwargs["args"]
            captured["prompt_text"] = kwargs["stdin_data"].decode("utf-8")
            result = runner.AgentResult()
            result.output = "ok"
            return result

        long_prompt = "# Task\n\n" + "\n".join(
            f"{idx}. /very/long/path/to/file_{idx}.c" for idx in range(5000)
        )

        with tempfile.TemporaryDirectory() as cwd:
            with patch.object(runner, "_find_pi_command", return_value=["/usr/bin/pi"]):
                with patch.object(runner, "_run_with_pi_retry", side_effect=fake_run_with_pi_retry):
                    result = asyncio.run(
                        runner.run_agent(
                            long_prompt,
                            model="test-model",
                            tools=["read"],
                            cwd=cwd,
                        )
                    )

        self.assertEqual(result.output, "ok")
        self.assertEqual(captured["prompt_text"], long_prompt)
        self.assertNotIn(long_prompt, captured["args"])

    def test_run_agent_retries_after_timeout(self):
        attempts = {"count": 0}

        async def fake_run_with_pi_retry(**kwargs):
            attempts["count"] += 1
            await asyncio.sleep(0.02)
            result = runner.AgentResult()
            result.output = "ok"
            return result

        with patch.object(runner, "_find_pi_command", return_value=["/usr/bin/pi"]):
            with patch.object(runner, "_run_with_pi_retry", side_effect=fake_run_with_pi_retry):
                result = asyncio.run(
                    runner.run_agent(
                        "hello",
                        model="test-model",
                        tools=["read"],
                        cwd=".",
                        run_timeout_seconds=0.01,
                        timeout_retry_enabled=True,
                        timeout_max_retries=1,
                        retry_delay=0,
                    )
                )

        self.assertEqual(attempts["count"], 2)
        self.assertIn("timed out", result.error or "")


if __name__ == "__main__":
    unittest.main()
