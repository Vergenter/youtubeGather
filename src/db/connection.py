from dataclasses import dataclass
from neo4j import GraphDatabase
from time import time
import logging
import datetime
import os


@dataclass(frozen=True)
class Result:
    total: int
    batches: int
    time: float


class Neo4jConnection:

    def __init__(self, uri=os.environ["NEO4J_BOLT_URL"], user=os.environ["NEO4J_USERNAMER"], pwd=os.environ["NEO4J_PASSWORD"]):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(
                self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.__driver.session(
                database=db) if db is not None else self.__driver.session()
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response

    def bulk_insert_data(self, query, rows, batch_size=10000):
        # Function to handle the updating the Neo4j database in batch mode.
        assert self.__driver is not None, "Driver not initialized!"

        total = 0
        total_bathes = (rows+batch_size-1)//batch_size
        batch = 0
        start = time()
        logging.info("[INSERTING] starting %s", datetime.datetime.now())
        while batch * batch_size < len(rows):
            current_batch = rows[batch*batch_size: (batch+1)*batch_size]
            res = self.query(query,
                             parameters={
                                 'rows': current_batch})
            total += len(current_batch)
            batch += 1
            logging.info(
                "[INSERTING][batch:%d/%d] total: %d duration from start: %d", batch, total_bathes, total, int(time()-start))

        return Result(total, batch, time()-start)
