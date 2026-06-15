import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.agent_process import AgentProcessHandle
from app import agent_process
from app import runner
from app.service import worker_service


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
    def test_materialize_task_pi_runtime_creates_role_scoped_dirs(self):
        cfg = SimpleNamespace(
            workers=SimpleNamespace(
                default_model="glm-5.1-180k",
                agents=[SimpleNamespace(model="glm-5.1-180k")],
                stage_models={},
                default_tools=["read"],
                default_thinking_level="off",
                system_prompt_dir="/tmp/workers",
            ),
            judges=SimpleNamespace(
                default_model="gpt-5.4",
                agents=[SimpleNamespace(model="gpt-5.4")],
                stage_models={"judge": "gpt-5.4"},
                default_tools=["read"],
                default_thinking_level="off",
                system_prompt_dir="/tmp/judges",
            ),
        )
        with tempfile.TemporaryDirectory() as task_root, tempfile.TemporaryDirectory() as pi_root:
            (Path(pi_root) / "settings.json").write_text('{"theme":"light"}', encoding="utf-8")
            (Path(pi_root) / "models.json").write_text(
                '{"providers":{"lite":{"models":[{"id":"glm-5.1-180k","contextWindow":128000,"contextLength":128000},{"id":"gpt-5.4","contextWindow":128000,"contextLength":128000}]}}}',
                encoding="utf-8",
            )
            with patch.dict(runner.os.environ, {"PI_CODING_AGENT_DIR": pi_root, "PI_MODELS_JSON": str(Path(pi_root) / "models.json")}, clear=False):
                role_dirs, mode = worker_service._materialize_task_pi_runtime(
                    task_root=task_root,
                    agent_task_key={"id": "key-1", "secret": "secret-1"},
                    cfg=cfg,
                )
            self.assertEqual(mode, "task_scoped")
            self.assertIn("workers", role_dirs)
            self.assertIn("judges", role_dirs)
            workers_dir = Path(role_dirs["workers"])
            judges_dir = Path(role_dirs["judges"])
            self.assertTrue((workers_dir / "models.json").is_file())
            self.assertTrue((workers_dir / "settings.json").is_file())
            self.assertTrue((judges_dir / "models.json").is_file())

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

    def test_cleanup_task_pi_processes_matches_cwd_prefix(self):
        fake_proc_dirs = [Path("/proc/101")]

        class FakePath(Path):
            _flavour = type(Path())._flavour

        proc_dir = FakePath("/proc/101")

        def fake_iterdir():
            return [proc_dir]

        def fake_read_text(self, encoding=None, errors=None):
            if str(self).endswith("/status"):
                return "Name:\tpi\nPPid:\t77\n"
            if str(self).endswith("/comm"):
                return "pi"
            return ""

        def fake_read_bytes(self):
            return b"pi --session /tmp/task-a/run/sessions/s1.jsonl"

        logs: list[str] = []
        with patch("app.agent_process.pathlib.Path.iterdir", side_effect=fake_iterdir):
            with patch("app.agent_process.pathlib.Path.read_text", fake_read_text):
                with patch("app.agent_process.pathlib.Path.read_bytes", fake_read_bytes):
                    with patch("app.agent_process.os.readlink") as readlink:
                        readlink.side_effect = lambda path: (
                            "/usr/bin/pi" if str(path).endswith("/exe") else "/tmp/task-a/run/workspace"
                        )
                        with patch("app.agent_process.subprocess.check_output", return_value="301"):
                            with patch("app.agent_process.os.killpg") as killpg:
                                killed = agent_process.cleanup_task_pi_processes(
                                    logs.append,
                                    label="test",
                                    task_id="task-a",
                                    task_roots=["/tmp/task-a"],
                                )
        self.assertIsNone(killed)
        self.assertTrue(any("task_id=task-a" in msg for msg in logs))

    def test_run_agent_uses_prompt_file_instead_of_raw_argv(self):
        captured = {}

        def fake_run_with_pi_retry(**kwargs):
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
                    result = asyncio.run(runner.run_agent(
                        long_prompt,
                        model="test-model",
                        tools=["read"],
                        cwd=cwd,
                    ))

        self.assertEqual(result.output, "ok")
        self.assertEqual(captured["prompt_text"], long_prompt)
        self.assertNotIn(long_prompt, captured["args"])

    def test_run_agent_retries_after_timeout(self):
        attempts = {"count": 0}

        def fake_run_with_pi_retry(**kwargs):
            attempts["count"] += 1
            result = runner.AgentResult()
            result.output = "ok"
            return result

        with patch.object(runner, "_find_pi_command", return_value=["/usr/bin/pi"]):
            with patch.object(runner, "_run_with_pi_retry", side_effect=fake_run_with_pi_retry):
                result = asyncio.run(runner.run_agent(
                    "hello",
                    model="test-model",
                    tools=["read"],
                    cwd=".",
                    run_timeout_seconds=0.01,
                    timeout_retry_enabled=True,
                    timeout_max_retries=1,
                    retry_delay=0,
                ))

        self.assertEqual(attempts["count"], 1)
        self.assertEqual(result.output, "ok")

    def test_run_agent_triggers_compaction_then_retries_on_context_overflow(self):
        prompts: list[str] = []

        def fake_run_with_pi_retry(**kwargs):
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

    def test_run_agent_preflight_without_session_fails_fast(self):
        oversized_prompt = "中" * 130000
        with tempfile.TemporaryDirectory() as cwd:
            with patch.object(runner, "_find_pi_command", return_value=["/usr/bin/pi"]):
                with patch.object(runner, "_run_with_pi_retry") as fake_retry:
                    result = asyncio.run(
                        runner.run_agent(
                            oversized_prompt,
                            model="glm-5.1-180k",
                            tools=["read"],
                            cwd=cwd,
                            session_file=None,
                            max_retries=0,
                            pi_max_retries=0,
                        )
                    )
        fake_retry.assert_not_called()
        self.assertTrue(result.context_budget_exceeded_preflight)
        self.assertTrue(result.context_overflow_failed_after_compaction)
        self.assertIn("75%", result.error or "")


if __name__ == "__main__":
    unittest.main()
