# S5 Deployment Architecture

## Database: AWS RDS (PostgreSQL 16)

- **Endpoint:** `bdi-hr-database.cx9wvz2cifc4.us-east-1.rds.amazonaws.com`
- **Port:** 5432
- **Database name:** `hr_database`
- **Engine:** PostgreSQL 16
- **Connection:** SSL required (`sslmode=require`)

The HR database contains 5 tables: `department`, `employee`, `project`, `employee_project`, and `salary_history`.

## Application: AWS EC2 (Amazon Linux 2023, t2.micro)

- **Public IP:** `54.91.118.220`
- **Port:** 8080
- **Runtime:** Python 3.12 + Uvicorn + FastAPI
- **API docs:** `http://54.91.118.220:8080/docs`

## How It Connects

The FastAPI application running on EC2 connects to the RDS instance using SQLAlchemy with a PostgreSQL connection URL passed via the `BDI_DB_URL` environment variable:

```
postgresql://postgres:<password>@bdi-hr-database.cx9wvz2cifc4.us-east-1.rds.amazonaws.com:5432/hr_database?sslmode=require
```

## Security Groups

| Resource | Rule | Port | Source |
|----------|------|------|--------|
| EC2 | Inbound SSH | 22 | My IP |
| EC2 | Inbound API | 8080 | 0.0.0.0/0 |
| RDS | Inbound PostgreSQL | 5432 | EC2 Security Group |

## Architecture Diagram

```
  Browser / Client
       │
       │ HTTP :8080
       ▼
  ┌─────────────┐
  │  EC2 t2.micro│  Amazon Linux 2023
  │  FastAPI +   │  Python 3.12
  │  Uvicorn     │
  └──────┬──────┘
         │ PostgreSQL :5432
         │ SSL (sslmode=require)
         ▼
  ┌─────────────┐
  │  AWS RDS    │  PostgreSQL 16
  │  hr_database│  us-east-1
  └─────────────┘
```
