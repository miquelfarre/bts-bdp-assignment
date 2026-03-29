import os
from datetime import datetime

import requests
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

s9 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s9",
    tags=["s9"],
)

_REPO = "bivanovski/bts-bdp-assignment"
_GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")


class PipelineRun(BaseModel):
    id: str
    repository: str
    branch: str
    status: str
    triggered_by: str
    started_at: datetime
    finished_at: datetime | None
    stages: list[str]


class PipelineStage(BaseModel):
    name: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    logs_url: str


def _headers() -> dict:
    h = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    if _GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {_GITHUB_TOKEN}"
    return h


def _map_status(run: dict) -> str:
    if run["status"] == "completed":
        conclusion = run.get("conclusion") or ""
        if conclusion == "success":
            return "success"
        if conclusion in ("failure", "timed_out", "startup_failure"):
            return "failure"
        return conclusion or "failure"
    if run["status"] == "in_progress":
        return "running"
    return "pending"


def _map_triggered_by(event: str) -> str:
    return {"push": "push", "pull_request": "pull_request", "schedule": "schedule"}.get(
        event, "manual"
    )


def _fetch_jobs(run_id: int) -> list[dict]:
    url = f"https://api.github.com/repos/{_REPO}/actions/runs/{run_id}/jobs"
    resp = requests.get(url, headers=_headers(), timeout=15)
    if not resp.ok:
        return []
    return resp.json().get("jobs", [])


def _run_to_pipeline(run: dict, jobs: list[dict]) -> PipelineRun:
    finished_at = None
    if run["status"] == "completed" and run.get("updated_at"):
        finished_at = datetime.fromisoformat(run["updated_at"].replace("Z", "+00:00"))

    return PipelineRun(
        id=str(run["id"]),
        repository=run["repository"]["name"],
        branch=run["head_branch"] or "",
        status=_map_status(run),
        triggered_by=_map_triggered_by(run.get("event", "")),
        started_at=datetime.fromisoformat(run["created_at"].replace("Z", "+00:00")),
        finished_at=finished_at,
        stages=[j["name"] for j in jobs],
    )


def _job_to_stage(job: dict, pipeline_id: str) -> PipelineStage:
    finished_at = None
    if job.get("completed_at"):
        finished_at = datetime.fromisoformat(job["completed_at"].replace("Z", "+00:00"))

    job_status = "running"
    if job["status"] == "completed":
        conclusion = job.get("conclusion") or "failure"
        job_status = "success" if conclusion == "success" else "failure"
    elif job["status"] == "queued":
        job_status = "pending"

    return PipelineStage(
        name=job["name"],
        status=job_status,
        started_at=datetime.fromisoformat(job["started_at"].replace("Z", "+00:00")),
        finished_at=finished_at,
        logs_url=f"/api/s9/pipelines/{pipeline_id}/stages/{job['name']}/logs",
    )


@s9.get("/pipelines")
def list_pipelines(
    repository: str | None = None,
    status_filter: str | None = None,
    num_results: int = 100,
    page: int = 0,
) -> list[PipelineRun]:
    """List CI/CD pipeline runs with their status.

    Returns a list of pipeline runs, optionally filtered by repository and status.
    Ordered by started_at descending (most recent first).
    Paginated with `num_results` per page and `page` number (0-indexed).
    """
    url = f"https://api.github.com/repos/{_REPO}/actions/runs"
    params = {"per_page": 100, "page": 1}
    resp = requests.get(url, headers=_headers(), params=params, timeout=15)
    if not resp.ok:
        return []

    runs = resp.json().get("workflow_runs", [])

    # Filter by status
    if status_filter:
        runs = [r for r in runs if _map_status(r) == status_filter]

    # Filter by repository name
    if repository:
        runs = [r for r in runs if r["repository"]["name"] == repository]

    # Sort by started_at descending (GitHub already returns newest first, but be explicit)
    runs.sort(key=lambda r: r["created_at"], reverse=True)

    # Apply pagination
    start = page * num_results
    runs = runs[start : start + num_results]

    pipelines = []
    for run in runs:
        jobs = _fetch_jobs(run["id"])
        pipelines.append(_run_to_pipeline(run, jobs))

    return pipelines


@s9.get("/pipelines/{pipeline_id}/stages")
def get_pipeline_stages(pipeline_id: str) -> list[PipelineStage]:
    """Get the stages of a specific pipeline run.

    Returns the stages in execution order.
    Each stage has a name, status, timestamps, and a logs URL.
    """
    try:
        run_id = int(pipeline_id)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Pipeline '{pipeline_id}' not found")

    jobs = _fetch_jobs(run_id)
    if not jobs:
        run_url = f"https://api.github.com/repos/{_REPO}/actions/runs/{run_id}"
        run_resp = requests.get(run_url, headers=_headers(), timeout=15)
        if run_resp.status_code == 404:
            raise HTTPException(status_code=404, detail=f"Pipeline '{pipeline_id}' not found")
        return []

    return [_job_to_stage(j, pipeline_id) for j in jobs]
