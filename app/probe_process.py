"""Independent kube probe process for secflow-app-entry-analyse."""

from app.probe_runtime import run_probe_server


def main() -> None:
    run_probe_server()


if __name__ == "__main__":
    main()
