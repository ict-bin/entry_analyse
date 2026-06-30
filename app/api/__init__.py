"""API router package for secflow-app-entry-analyse."""

from fastapi import APIRouter

router = APIRouter(prefix="/api/app/entry-analyse")

from . import tasks, prompts, config, debug_reports  # noqa: E402, F401
