from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import NoHostAvailable, OperationTimedOut, InvalidRequest
from fastapi import HTTPException

class CassandraStorage:
    def __init__(self):
        self.cluster = None
        self.session = None
        self.keyspace = "timeseries"
        self.table = "events"

    def initialize(self):
        try:
            self.cluster = Cluster(['localhost'])  
            self.session = self.cluster.connect()

            self.session.execute("""
                CREATE KEYSPACE IF NOT EXISTS timeseries
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """)
            self.session.set_keyspace(self.keyspace)

            self.session.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    timestamp timestamp,
                    metric text,
                    value float,
                    PRIMARY KEY (metric, timestamp)
                ) WITH CLUSTERING ORDER BY (timestamp DESC)
            """)

        except NoHostAvailable as e:
            raise HTTPException(status_code=503, detail="Cassandra cluster is unavailable")
        except InvalidRequest as e:
            raise HTTPException(status_code=500, detail=f"Failed to initialize Cassandra: {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    def _check_session(self):
        if not self.session or self.session.is_shutdown:
            raise HTTPException(status_code=500, detail="Cassandra session not initialized")

    def get_events(self, limit: int = 10, metric: str = "temperature"):
        self._check_session()

        try:
            query = f"SELECT timestamp, metric, value FROM {self.table} WHERE metric = %s LIMIT %s"
            params = [metric, limit]

            statement = SimpleStatement(query)
            rows = self.session.execute(statement, params, timeout=10.0) 
            events = [
                {
                    "timestamp": row.timestamp.isoformat() + "Z",
                    "metric": row.metric,
                    "value": row.value
                }
                for row in rows
            ]
            return events

        except NoHostAvailable as e:
            raise HTTPException(status_code=503, detail="Cassandra cluster is unavailable")
        except OperationTimedOut as e:
            raise HTTPException(status_code=504, detail="Query timed out while fetching events")
        except InvalidRequest as e:
            raise HTTPException(status_code=400, detail="Invalid query parameters")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    def get_latest_events(self, last_timestamp: str = None, limit: int = 10, metric: str = "temperature"):
        self._check_session()
        try:
            query = f"SELECT timestamp, metric, value FROM {self.table} WHERE metric = %s"
            params = [metric]

            if last_timestamp:
                try:
                    from datetime import datetime
                    datetime.fromisoformat(last_timestamp.replace("Z", "+00:00"))
                    query += " AND timestamp > %s"
                    params.append(last_timestamp)
                except ValueError:
                    raise HTTPException(status_code=400, detail="Invalid timestamp format")

            query += " LIMIT %s"
            params.append(limit)

            statement = SimpleStatement(query)
            rows = self.session.execute(statement, params, timeout=10.0)
            events = [
                {
                    "timestamp": row.timestamp.isoformat() + "Z",
                    "metric": row.metric,
                    "value": row.value
                }
                for row in rows
            ]
            return sorted(events, key=lambda x: x["timestamp"])  

        except NoHostAvailable as e:
            raise HTTPException(status_code=503, detail="Cassandra cluster is unavailable")
        except OperationTimedOut as e:
            raise HTTPException(status_code=504, detail="Query timed out while fetching events")
        except InvalidRequest as e:
            raise HTTPException(status_code=400, detail="Invalid query parameters")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    def shutdown(self):
        """Gracefully shut down the Cassandra connection."""
        try:
            if self.cluster and not self.cluster.is_shutdown:
                self.cluster.shutdown()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error shutting down Cassandra: {str(e)}")