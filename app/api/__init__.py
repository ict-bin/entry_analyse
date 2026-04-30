"""API router package for secflow-app-entry-analyse."""

from fastapi import APIRouter

router = APIRouter(prefix="/api/app/entry-analyse")

from . import tasks, prompts, config  # noqa: E402, F401
