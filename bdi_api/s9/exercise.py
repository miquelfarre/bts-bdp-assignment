from datetime import datetime, timedelta, timezone
from typing import Optional

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


class PipelineRun(BaseModel):
    id: str
    repository: str
    branch: str
    status: str
    triggered_by: str
    started_at: datetime
    finished_at: Optional[datetime]
    stages: list[str]


class PipelineStage(BaseModel):
    name: str
    status: str
    started_at: datetime
    finished_at: Optional[datetime]
    logs_url: str


def _generate_pipelines() -> list[dict]:
    """Generate realistic CI/CD pipeline data."""
    repos = ["frontend-app", "backend-api", "data-pipeline", "infra-config"]
    branches = ["main", "develop", "feature/auth", "fix/bug-123"]
    statuses = ["success", "failure", "success", "success", "running", "pending", "success", "success"]
    triggers = ["push", "pull_request", "schedule", "manual", "push", "push", "pull_request", "push"]
    stage_sets = [
        ["lint", "test", "build", "deploy"],
        ["lint", "test", "build"],
        ["lint", "test"],
        ["lint", "test", "build", "deploy"],
    ]

    pipelines = []
    base_time = datetime(2026, 4, 7, 12, 0, 0, tzinfo=timezone.utc)

    for i in range(25):
        started = base_time - timedelta(hours=i * 2, minutes=i * 7)
        pipeline_status = statuses[i % len(statuses)]
        stages = stage_sets[i % len(stage_sets)]

        if pipeline_status in ("success", "failure"):
            duration = timedelta(minutes=5 + (i % 10))
            finished = started + duration
        else:
            finished = None

        pipelines.append({
            "id": f"pipe-{i + 1:04d}",
            "repository": repos[i % len(repos)],
            "branch": branches[i % len(branches)],
            "status": pipeline_status,
            "triggered_by": triggers[i % len(triggers)],
            "started_at": started.isoformat(),
            "finished_at": finished.isoformat() if finished else None,
            "stages": stages,
        })

    return pipelines


def _generate_stages(pipeline: dict) -> list[dict]:
    """Generate stage details for a pipeline."""
    stages = []
    started = datetime.fromisoformat(pipeline["started_at"])
    pipeline_status = pipeline["status"]

    for j, stage_name in enumerate(pipeline["stages"]):
        stage_start = started + timedelta(minutes=j * 2)

        if pipeline_status == "pending":
            stage_status = "pending"
            stage_finished = None
        elif pipeline_status == "running" and j == len(pipeline["stages"]) - 1:
            stage_status = "running"
            stage_finished = None
        elif pipeline_status == "failure" and j == len(pipeline["stages"]) - 1:
            stage_status = "failure"
            stage_finished = stage_start + timedelta(minutes=1, seconds=30)
        else:
            stage_status = "success"
            stage_finished = stage_start + timedelta(minutes=1, seconds=45)

        stages.append({
            "name": stage_name,
            "status": stage_status,
            "started_at": stage_start.isoformat(),
            "finished_at": stage_finished.isoformat() if stage_finished else None,
            "logs_url": f"https://ci.example.com/logs/{pipeline['id']}/{stage_name}",
        })

    return stages


# Generate data once at module load
PIPELINES = _generate_pipelines()
PIPELINE_STAGES = {p["id"]: _generate_stages(p) for p in PIPELINES}


@s9.get("/pipelines")
def list_pipelines(
    repository: Optional[str] = None,
    status_filter: Optional[str] = None,
    num_results: int = 100,
    page: int = 0,
) -> list[PipelineRun]:
    """List CI/CD pipeline runs with their status.

    Returns a list of pipeline runs, optionally filtered by repository and status.
    Ordered by started_at descending (most recent first).
    Paginated with `num_results` per page and `page` number (0-indexed).

    Valid statuses: "success", "failure", "running", "pending"
    Valid triggered_by values: "push", "pull_request", "schedule", "manual"
    """
    filtered = PIPELINES

    if repository:
        filtered = [p for p in filtered if p["repository"] == repository]
    if status_filter:
        filtered = [p for p in filtered if p["status"] == status_filter]

    # Sort by started_at descending
    filtered = sorted(filtered, key=lambda p: p["started_at"], reverse=True)

    # Paginate
    start = page * num_results
    end = start + num_results
    return [PipelineRun(**p) for p in filtered[start:end]]


@s9.get("/pipelines/{pipeline_id}/stages")
def get_pipeline_stages(pipeline_id: str) -> list[PipelineStage]:
    """Get the stages of a specific pipeline run.

    Returns the stages in execution order.
    Each stage has a name, status, timestamps, and a logs URL.

    Typical stages: "lint", "test", "build", "deploy"
    """
    if pipeline_id not in PIPELINE_STAGES:
        raise HTTPException(status_code=404, detail=f"Pipeline '{pipeline_id}' not found")

    return [PipelineStage(**s) for s in PIPELINE_STAGES[pipeline_id]]
