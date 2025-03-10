from neo4j import GraphDatabase

uri = "bolt://106.14.88.25:7687"
user = "neo4j"
password = "JcterbFKHB8CAN0W"

driver = GraphDatabase.driver(uri, auth=(user, password))

with driver.session() as session:
    result = session.run("MATCH (n) RETURN COUNT(n)")
    for record in result:
        print(record)

driver.close()
