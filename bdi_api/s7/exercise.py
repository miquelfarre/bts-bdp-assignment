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


class PersonCreate(BaseModel):
    name: str
    city: str
    age: int


class RelationshipCreate(BaseModel):
    from_person: str
    to_person: str
    relationship_type: str = "FRIENDS_WITH"


def _get_driver():
    return GraphDatabase.driver(
        settings.neo4j_url,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )


@s7.post("/graph/person")
def create_person(person: PersonCreate) -> dict:
    """Create a person node in Neo4J.

    Use the BDI_NEO4J_URL environment variable to configure the connection.
    Start Neo4J with: make neo4j
    """
    driver = _get_driver()
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

    Each result should include: name, city, age.
    """
    driver = _get_driver()
    with driver.session() as session:
        result = session.run("MATCH (p:Person) RETURN p.name AS name, p.city AS city, p.age AS age")
        persons = [{"name": r["name"], "city": r["city"], "age": r["age"]} for r in result]
    driver.close()
    return persons


@s7.get("/graph/person/{name}/friends")
def get_friends(name: str) -> list[dict]:
    """Get friends of a person.

    Returns all persons connected by a FRIENDS_WITH relationship (any direction).
    If person not found, return 404.
    """
    driver = _get_driver()
    with driver.session() as session:
        # Check if person exists
        check = session.run(
            "MATCH (p:Person {name: $name}) RETURN p",
            name=name,
        )
        if not check.single():
            driver.close()
            raise HTTPException(status_code=404, detail=f"Person '{name}' not found")

        result = session.run(
            """
            MATCH (p:Person {name: $name})-[:FRIENDS_WITH]-(friend:Person)
            RETURN friend.name AS name, friend.city AS city, friend.age AS age
            """,
            name=name,
        )
        friends = [{"name": r["name"], "city": r["city"], "age": r["age"]} for r in result]
    driver.close()
    return friends


@s7.post("/graph/relationship")
def create_relationship(rel: RelationshipCreate) -> dict:
    """Create a relationship between two persons.

    Both persons must exist. Returns 404 if either is not found.
    """
    driver = _get_driver()
    with driver.session() as session:
        # Verify both persons exist
        result = session.run(
            """
            OPTIONAL MATCH (a:Person {name: $from_person})
            OPTIONAL MATCH (b:Person {name: $to_person})
            RETURN a IS NOT NULL AS a_exists, b IS NOT NULL AS b_exists
            """,
            from_person=rel.from_person,
            to_person=rel.to_person,
        )
        record = result.single()
        if not record["a_exists"]:
            driver.close()
            raise HTTPException(status_code=404, detail=f"Person '{rel.from_person}' not found")
        if not record["b_exists"]:
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

    Recommend friends-of-friends who are NOT already direct friends.
    Return them sorted by number of mutual friends (descending).
    If person not found, return 404.

    Each result should include: name, city, mutual_friends (count).
    """
    driver = _get_driver()
    with driver.session() as session:
        # Check if person exists
        check = session.run(
            "MATCH (p:Person {name: $name}) RETURN p",
            name=name,
        )
        if not check.single():
            driver.close()
            raise HTTPException(status_code=404, detail=f"Person '{name}' not found")

        result = session.run(
            """
            MATCH (p:Person {name: $name})-[:FRIENDS_WITH]-(friend)-[:FRIENDS_WITH]-(fof:Person)
            WHERE fof <> p AND NOT (p)-[:FRIENDS_WITH]-(fof)
            RETURN fof.name AS name, fof.city AS city, COUNT(DISTINCT friend) AS mutual_friends
            ORDER BY mutual_friends DESC
            """,
            name=name,
        )
        recommendations = [
            {"name": r["name"], "city": r["city"], "mutual_friends": r["mutual_friends"]}
            for r in result
        ]
    driver.close()
    return recommendations
