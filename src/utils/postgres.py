import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import JSON
from sqlalchemy.exc import OperationalError


class Postgres:
    def __init__(self, dns: dict):

        self.dns = dns
        self.engine = create_engine(
            f"postgresql://{self.dns['user']}:{self.dns['password']}@{self.dns['host']}:{self.dns['port']}/{self.dns['database']}"
        )
        # self.session = self.engine.connect()
        # self.metadata = self.engine.metadata
        # self.metadata.create_all(self.engine)

    def append_df(self, df: pd.DataFrame, table: str):

        conn = self.engine.connect()
        conn.autocommit = True

        df.to_sql(
            table,
            schema=self.dns.get("schema"),
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            dtype={"parameters": JSON},
        )
        conn.close()

    def test_connection(self) -> bool:
        """Check if the database is connectable"""
        try:
            self.engine.execute("SELECT 1")
        except OperationalError:
            print("Database check failed")
            return False
        print("Database check succeeded")
        return True
