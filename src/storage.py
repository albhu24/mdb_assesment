from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

class CassandraStorage:
    def __init__(self):
        self.cluster = None
        self.session = None
        self.keyspace = "timeseries"
        self.table = "events"

    def initialize(self):
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

    def get_events(self, limit=10):
    

    def get_latest_events(self, last_timestamp=None, limit=10):
      

    def shutdown(self):
        if self.cluster:
            self.cluster.shutdown()