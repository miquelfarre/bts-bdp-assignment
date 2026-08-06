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


def _get_engine():
    return create_engine(settings.db_url)


@s5.post("/db/init")
def init_database() -> str:
    """Create all HR database tables (department, employee, project,
    employee_project, salary_history) with their relationships and indexes.

    Use the BDI_DB_URL environment variable to configure the database connection.
    Default: sqlite:///hr_database.db
    """
    engine = _get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS department (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                location VARCHAR(100) NOT NULL
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS employee (
                id INTEGER PRIMARY KEY,
                first_name VARCHAR(100) NOT NULL,
                last_name VARCHAR(100) NOT NULL,
                email VARCHAR(200) NOT NULL,
                salary REAL NOT NULL,
                hire_date DATE NOT NULL,
                department_id INTEGER REFERENCES department(id)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS project (
                id INTEGER PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                budget REAL NOT NULL,
                department_id INTEGER REFERENCES department(id)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS employee_project (
                employee_id INTEGER REFERENCES employee(id),
                project_id INTEGER REFERENCES project(id),
                PRIMARY KEY (employee_id, project_id)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS salary_history (
                id INTEGER PRIMARY KEY,
                employee_id INTEGER REFERENCES employee(id),
                change_date DATE NOT NULL,
                old_salary REAL NOT NULL,
                new_salary REAL NOT NULL,
                reason VARCHAR(200)
            )
        """))
    return "OK"


@s5.post("/db/seed")
def seed_database() -> str:
    """Populate the HR database with sample data.

    Inserts departments, employees, projects, assignments, and salary history.
    """
    engine = _get_engine()
    with engine.begin() as conn:
        # Clear existing data (reverse order for FK constraints)
        conn.execute(text("DELETE FROM salary_history"))
        conn.execute(text("DELETE FROM employee_project"))
        conn.execute(text("DELETE FROM project"))
        conn.execute(text("DELETE FROM employee"))
        conn.execute(text("DELETE FROM department"))

        # Departments
        conn.execute(text("""
            INSERT INTO department (id, name, location) VALUES
            (1, 'Engineering', 'Barcelona'),
            (2, 'Marketing', 'Madrid'),
            (3, 'Finance', 'London'),
            (4, 'HR', 'Berlin')
        """))

        # Employees
        conn.execute(text("""
            INSERT INTO employee (id, first_name, last_name, email, salary, hire_date, department_id) VALUES
            (1, 'Alice', 'Smith', 'alice.smith@company.com', 85000, '2020-01-15', 1),
            (2, 'Bob', 'Johnson', 'bob.johnson@company.com', 72000, '2019-06-01', 1),
            (3, 'Carol', 'Williams', 'carol.williams@company.com', 65000, '2021-03-20', 2),
            (4, 'David', 'Brown', 'david.brown@company.com', 90000, '2018-11-10', 3),
            (5, 'Eve', 'Davis', 'eve.davis@company.com', 55000, '2022-07-01', 4),
            (6, 'Frank', 'Miller', 'frank.miller@company.com', 78000, '2020-09-15', 1),
            (7, 'Grace', 'Wilson', 'grace.wilson@company.com', 62000, '2021-01-10', 2),
            (8, 'Hank', 'Moore', 'hank.moore@company.com', 95000, '2017-04-22', 3),
            (9, 'Ivy', 'Taylor', 'ivy.taylor@company.com', 58000, '2023-02-28', 4),
            (10, 'Jack', 'Anderson', 'jack.anderson@company.com', 81000, '2019-12-05', 1)
        """))

        # Projects
        conn.execute(text("""
            INSERT INTO project (id, name, budget, department_id) VALUES
            (1, 'Project Alpha', 500000, 1),
            (2, 'Project Beta', 300000, 1),
            (3, 'Campaign Q1', 150000, 2),
            (4, 'Annual Audit', 200000, 3),
            (5, 'Recruitment Drive', 100000, 4)
        """))

        # Employee-Project assignments
        conn.execute(text("""
            INSERT INTO employee_project (employee_id, project_id) VALUES
            (1, 1), (1, 2), (2, 1), (6, 2), (10, 1),
            (3, 3), (7, 3),
            (4, 4), (8, 4),
            (5, 5), (9, 5)
        """))

        # Salary history
        conn.execute(text("""
            INSERT INTO salary_history (id, employee_id, change_date, old_salary, new_salary, reason) VALUES
            (1, 1, '2021-01-15', 75000, 80000, 'Annual review'),
            (2, 1, '2022-01-15', 80000, 85000, 'Promotion'),
            (3, 2, '2020-06-01', 65000, 70000, 'Annual review'),
            (4, 2, '2021-06-01', 70000, 72000, 'Annual review'),
            (5, 4, '2019-11-10', 80000, 85000, 'Annual review'),
            (6, 4, '2020-11-10', 85000, 90000, 'Promotion'),
            (7, 8, '2018-04-22', 85000, 90000, 'Annual review'),
            (8, 8, '2019-04-22', 90000, 95000, 'Promotion')
        """))

    return "OK"


@s5.get("/departments/")
def list_departments() -> list[dict]:
    """Return all departments.

    Each department should include: id, name, location
    """
    engine = _get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT id, name, location FROM department ORDER BY id")).fetchall()
    return [{"id": r[0], "name": r[1], "location": r[2]} for r in rows]


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
    engine = _get_engine()
    offset = (page - 1) * per_page
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT e.id, e.first_name, e.last_name, e.email, e.salary, d.name as department_name
                FROM employee e
                JOIN department d ON e.department_id = d.id
                ORDER BY e.id
                LIMIT :limit OFFSET :offset
            """),
            {"limit": per_page, "offset": offset},
        ).fetchall()
    return [
        {
            "id": r[0], "first_name": r[1], "last_name": r[2],
            "email": r[3], "salary": r[4], "department_name": r[5],
        }
        for r in rows
    ]


@s5.get("/departments/{dept_id}/employees")
def list_department_employees(dept_id: int) -> list[dict]:
    """Return all employees in a specific department.

    Each employee should include: id, first_name, last_name, email, salary, hire_date
    """
    engine = _get_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT id, first_name, last_name, email, salary, hire_date
                FROM employee
                WHERE department_id = :dept_id
                ORDER BY id
            """),
            {"dept_id": dept_id},
        ).fetchall()
    return [
        {
            "id": r[0], "first_name": r[1], "last_name": r[2],
            "email": r[3], "salary": r[4], "hire_date": str(r[5]),
        }
        for r in rows
    ]


@s5.get("/departments/{dept_id}/stats")
def department_stats(dept_id: int) -> dict:
    """Return KPI statistics for a department.

    Response should include: department_name, employee_count, avg_salary, project_count
    """
    engine = _get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT
                    d.name as department_name,
                    COUNT(DISTINCT e.id) as employee_count,
                    AVG(e.salary) as avg_salary,
                    COUNT(DISTINCT p.id) as project_count
                FROM department d
                LEFT JOIN employee e ON e.department_id = d.id
                LEFT JOIN project p ON p.department_id = d.id
                WHERE d.id = :dept_id
                GROUP BY d.id, d.name
            """),
            {"dept_id": dept_id},
        ).fetchone()
    if not row:
        return {}
    return {
        "department_name": row[0],
        "employee_count": row[1],
        "avg_salary": row[2],
        "project_count": row[3],
    }


@s5.get("/employees/{emp_id}/salary-history")
def salary_history(emp_id: int) -> list[dict]:
    """Return the salary evolution for an employee, ordered by date.

    Each entry should include: change_date, old_salary, new_salary, reason
    """
    engine = _get_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT change_date, old_salary, new_salary, reason
                FROM salary_history
                WHERE employee_id = :emp_id
                ORDER BY change_date
            """),
            {"emp_id": emp_id},
        ).fetchall()
    return [
        {
            "change_date": str(r[0]),
            "old_salary": r[1],
            "new_salary": r[2],
            "reason": r[3],
        }
        for r in rows
    ]
