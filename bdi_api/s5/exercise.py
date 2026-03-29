from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Query
from sqlalchemy import create_engine, text

from bdi_api.settings import Settings

settings = Settings()

s5 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s5",
    tags=["s5"],
)

_SQL_DIR = Path(__file__).parent
_SCHEMA_FILE = _SQL_DIR / "hr_schema.sql"
_SEED_FILE = _SQL_DIR / "hr_seed_data.sql"


def get_engine():
    return create_engine(settings.db_url)


def _is_sqlite() -> bool:
    return settings.db_url.startswith("sqlite")


@s5.post("/db/init")
def init_database() -> str:
    """Create all HR database tables (department, employee, project,
    employee_project, salary_history) with their relationships and indexes.

    Use the BDI_DB_URL environment variable to configure the database connection.
    Default: sqlite:///hr_database.db
    """
    schema_sql = _SCHEMA_FILE.read_text()
    if _is_sqlite():
        # SQLite: strip CASCADE only from DROP TABLE lines, and replace SERIAL type
        fixed_lines = []
        for line in schema_sql.splitlines():
            if line.strip().upper().startswith("DROP TABLE"):
                line = line.replace(" CASCADE", "")
            fixed_lines.append(line)
        schema_sql = "\n".join(fixed_lines).replace("SERIAL", "INTEGER")
    engine = get_engine()
    with engine.begin() as conn:
        for statement in schema_sql.strip().split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt))
    return "OK"


@s5.post("/db/seed")
def seed_database() -> str:
    """Populate the HR database with sample data.

    Inserts departments, employees, projects, assignments, and salary history.
    """
    seed_sql = _SEED_FILE.read_text()
    engine = get_engine()
    with engine.begin() as conn:
        for statement in seed_sql.strip().split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt))
    return "OK"


@s5.get("/departments/")
def list_departments() -> list[dict]:
    """Return all departments.

    Each department should include: id, name, location
    """
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT id, name, location FROM department ORDER BY id")).mappings().all()
    return [dict(row) for row in rows]


@s5.get("/employees/")
def list_employees(
    page: Annotated[
        int,
        Query(description="Page number (1-indexed)", ge=1),
    ] = 1,
    per_page: Annotated[
        int,
        Query(description="Number of employees per page", ge=1, le=100),
    ] = 10,
) -> list[dict]:
    """Return employees with their department name, paginated.

    Each employee should include: id, first_name, last_name, email, salary, department_name
    """
    offset = (page - 1) * per_page
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT e.id, e.first_name, e.last_name, e.email, e.salary,
                       d.name AS department_name
                FROM employee e
                LEFT JOIN department d ON e.department_id = d.id
                ORDER BY e.id
                LIMIT :limit OFFSET :offset
                """
            ),
            {"limit": per_page, "offset": offset},
        ).mappings().all()
    return [dict(row) for row in rows]


@s5.get("/departments/{dept_id}/employees")
def list_department_employees(dept_id: int) -> list[dict]:
    """Return all employees in a specific department.

    Each employee should include: id, first_name, last_name, email, salary, hire_date
    """
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, first_name, last_name, email, salary, hire_date
                FROM employee
                WHERE department_id = :dept_id
                ORDER BY id
                """
            ),
            {"dept_id": dept_id},
        ).mappings().all()
    return [dict(row) for row in rows]


@s5.get("/departments/{dept_id}/stats")
def department_stats(dept_id: int) -> dict:
    """Return KPI statistics for a department.

    Response should include: department_name, employee_count, avg_salary, project_count
    """
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                    d.name AS department_name,
                    COUNT(DISTINCT e.id) AS employee_count,
                    AVG(e.salary) AS avg_salary,
                    COUNT(DISTINCT p.id) AS project_count
                FROM department d
                LEFT JOIN employee e ON e.department_id = d.id
                LEFT JOIN project p ON p.department_id = d.id
                WHERE d.id = :dept_id
                GROUP BY d.id, d.name
                """
            ),
            {"dept_id": dept_id},
        ).mappings().first()
    if row is None:
        return {}
    result = dict(row)
    # Ensure avg_salary is a float (or None) rather than a Decimal
    if result.get("avg_salary") is not None:
        result["avg_salary"] = float(result["avg_salary"])
    return result


@s5.get("/employees/{emp_id}/salary-history")
def salary_history(emp_id: int) -> list[dict]:
    """Return the salary evolution for an employee, ordered by date.

    Each entry should include: change_date, old_salary, new_salary, reason
    """
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT change_date, old_salary, new_salary, reason
                FROM salary_history
                WHERE employee_id = :emp_id
                ORDER BY change_date
                """
            ),
            {"emp_id": emp_id},
        ).mappings().all()
    return [dict(row) for row in rows]
