from fastapi import APIRouter, HTTPException, status
from neo4j import GraphDatabase
from pydantic import BaseModel

from bdi_api.settings import Settings

settings = Settings()

s7 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s7",
    tags=["s7"],
)


def get_driver():
    return GraphDatabase.driver(
        settings.neo4j_url,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )


class PersonCreate(BaseModel):
    name: str
    city: str
    age: int


class RelationshipCreate(BaseModel):
    from_person: str
    to_person: str
    relationship_type: str = "FRIENDS_WITH"


@s7.post("/graph/person")
def create_person(person: PersonCreate) -> dict:
    """Create a person node in Neo4J."""
    driver = get_driver()
    with driver.session() as session:
        session.run(
            "MERGE (p:Person {name: $name}) SET p.city = $city, p.age = $age",
            name=person.name,
            city=person.city,
            age=person.age,
        )
    driver.close()
    return {"status": "ok", "name": person.name}


@s7.get("/graph/persons")
def list_persons() -> list[dict]:
    """List all person nodes.

    Each result includes: name, city, age.
    """
    driver = get_driver()
    with driver.session() as session:
        result = session.run("MATCH (p:Person) RETURN p")
        persons = [
            {"name": record["p"]["name"], "city": record["p"]["city"], "age": record["p"]["age"]}
            for record in result
        ]
    driver.close()
    return persons


@s7.get("/graph/person/{name}/friends")
def get_friends(name: str) -> list[dict]:
    """Get friends of a person.

    Returns all persons connected by a FRIENDS_WITH relationship (any direction).
    Returns 404 if person not found.
    """
    driver = get_driver()
    with driver.session() as session:
        exists = session.run("MATCH (p:Person {name: $name}) RETURN p", name=name).single()
        if not exists:
            driver.close()
            raise HTTPException(status_code=404, detail=f"Person '{name}' not found")
        result = session.run(
            "MATCH (p:Person {name: $name})-[:FRIENDS_WITH]-(friend:Person) RETURN friend",
            name=name,
        )
        friends = [
            {"name": record["friend"]["name"], "city": record["friend"]["city"], "age": record["friend"]["age"]}
            for record in result
        ]
    driver.close()
    return friends


@s7.post("/graph/relationship")
def create_relationship(rel: RelationshipCreate) -> dict:
    """Create a FRIENDS_WITH relationship between two persons.

    Both persons must exist. Returns 404 if either is not found.
    """
    driver = get_driver()
    with driver.session() as session:
        if not session.run("MATCH (p:Person {name: $name}) RETURN p", name=rel.from_person).single():
            driver.close()
            raise HTTPException(status_code=404, detail=f"Person '{rel.from_person}' not found")
        if not session.run("MATCH (p:Person {name: $name}) RETURN p", name=rel.to_person).single():
            driver.close()
            raise HTTPException(status_code=404, detail=f"Person '{rel.to_person}' not found")
        session.run(
            """
            MATCH (a:Person {name: $from_person}), (b:Person {name: $to_person})
            MERGE (a)-[:FRIENDS_WITH]->(b)
            """,
            from_person=rel.from_person,
            to_person=rel.to_person,
        )
    driver.close()
    return {"status": "ok", "from": rel.from_person, "to": rel.to_person}


@s7.get("/graph/person/{name}/recommendations")
def get_recommendations(name: str) -> list[dict]:
    """Get friend recommendations for a person.

    Recommends friends-of-friends not already direct friends,
    sorted by mutual friend count (descending).
    Returns 404 if person not found.

    Each result includes: name, city, age, mutual_friends (count).
    """
    driver = get_driver()
    with driver.session() as session:
        exists = session.run("MATCH (p:Person {name: $name}) RETURN p", name=name).single()
        if not exists:
            driver.close()
            raise HTTPException(status_code=404, detail=f"Person '{name}' not found")
        result = session.run(
            """
            MATCH (p:Person {name: $name})-[:FRIENDS_WITH]-(friend:Person)-[:FRIENDS_WITH]-(rec:Person)
            WHERE rec.name <> $name
              AND NOT (p)-[:FRIENDS_WITH]-(rec)
            RETURN rec, count(friend) AS mutual_friends
            ORDER BY mutual_friends DESC
            """,
            name=name,
        )
        recommendations = [
            {
                "name": record["rec"]["name"],
                "city": record["rec"]["city"],
                "age": record["rec"]["age"],
                "mutual_friends": record["mutual_friends"],
            }
            for record in result
        ]
    driver.close()
    return recommendations
