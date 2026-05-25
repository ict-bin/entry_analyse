import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.agent_process import AgentProcessHandle
from app import agent_process
from app import runner


def _overflow_result() -> runner.AgentResult:
    result = runner.AgentResult()
    result.exit_code = 1
    result.error = (
        "400 litellm.BadRequestError: Hosted_vllmException - "
        '{"error":{"message":"You passed 147421 input tokens and requested 16384 output tokens. '
        "However, the model's context length is only 163804 tokens, resulting in a maximum input "
        'length of 147420 tokens. Please reduce the length of the input prompt."}}'
    )
    return result


class RunAgentPromptFileTests(unittest.TestCase):
    def test_cleanup_orphan_pi_processes_skips_business_pid1_container(self):
        with patch.object(agent_process, "_pid1_is_reaper_process", return_value=False):
            killed = agent_process.cleanup_orphan_pi_processes(lambda _: None, label="test")
        self.assertEqual(killed, 0)

    def test_pid1_reaper_detection_rejects_python_main(self):
        with patch.object(agent_process, "_read_proc_name", return_value="python3"):
            with patch("app.agent_process.os.readlink", return_value="/usr/bin/python3"):
                self.assertFalse(agent_process._pid1_is_reaper_process())

    def test_pid1_reaper_detection_accepts_tini(self):
        with patch.object(agent_process, "_read_proc_name", return_value="tini"):
            self.assertTrue(agent_process._pid1_is_reaper_process())

    def test_agent_process_terminate_tree_force_cleans_group_after_exit(self):
        logs: list[str] = []

        class FakeProc:
            pid = 123
            returncode = 0

            async def wait(self):
                return 0

        async def scenario():
            with patch("app.agent_process.process_group_exists", return_value=True):
                with patch("app.agent_process.os.killpg") as killpg:
                    handle = AgentProcessHandle(
                        proc=FakeProc(),
                        label="test",
                        logger=logs.append,
                        pgid=456,
                    )
                    await handle.terminate_tree(reason="cleanup")
                    killpg.assert_called_once()

        asyncio.run(scenario())
        self.assertTrue(any("cleaning leaked pi process group" in msg for msg in logs))

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

    def test_run_agent_triggers_compaction_then_retries_on_context_overflow(self):
        prompts: list[str] = []

        async def fake_run_with_pi_retry(**kwargs):
            prompts.append(kwargs["stdin_data"].decode("utf-8"))
            if len(prompts) == 1:
                return _overflow_result()
            result = runner.AgentResult()
            result.output = "ok"
            result.exit_code = 0
            return result

        with tempfile.TemporaryDirectory() as cwd:
            with patch.object(runner, "_find_pi_command", return_value=["/usr/bin/pi"]):
                with patch.object(runner, "_run_with_pi_retry", side_effect=fake_run_with_pi_retry):
                    result = asyncio.run(
                        runner.run_agent(
                            "summary",
                            model="MiniMax/MiniMax-M2.5",
                            tools=["read"],
                            cwd=cwd,
                            session_file="/tmp/test-session.jsonl",
                            max_retries=0,
                            pi_max_retries=0,
                        )
                    )

        self.assertEqual(result.output, "ok")
        self.assertEqual(len(prompts), 3)
        self.assertEqual(prompts[0], "summary")
        self.assertIn("compaction", prompts[1].lower())
        self.assertEqual(prompts[2], "summary")


if __name__ == "__main__":
    unittest.main()
