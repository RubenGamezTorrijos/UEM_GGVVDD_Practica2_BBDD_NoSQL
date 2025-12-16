
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.neo4j.database import Neo4jManager

def main():
    try:
        neo4j = Neo4jManager()
        
        # Clear existing
        with neo4j.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("Cleared existing graph")

        # Import Business
        print("Importing Business...")
        neo4j.import_data("business_neo4j.csv") 

        # Import Users
        print("Importing Users...")
        neo4j.import_data("user_neo4j.csv")
        
        # Import Relations
        print("Importing Relationships...")
        neo4j.create_graph_relationships("review_neo4j.csv")
        
        print("Neo4j Import Complete")
        neo4j.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
