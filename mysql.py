import os

from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine

LOCALHOST = "localhost"


def get_engine(tunnel) -> Engine:
    return create_engine(
        URL.create(
            drivername="mysql+pymysql",
            username=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PWD"),
            host=LOCALHOST,
            port=tunnel.local_bind_port,
            database=os.getenv("MYSQL_DB"),
        ),
    )


def get_data(engine: Engine, query: str) -> list:
    with engine.connect() as conn:
        results = conn.execute(query)
        return [dict(zip(results.keys(), result)) for result in results.fetchall()]


def get(query: str) -> list:
    with SSHTunnelForwarder(
        (os.getenv("SSH_HOST"), int(os.getenv("SSH_PORT"))),
        ssh_username=os.getenv("SSH_USER"),
        ssh_password=os.getenv("SSH_PWD"),
        remote_bind_address=(LOCALHOST, 3306),
    ) as tunnel:
        return get_data(get_engine(tunnel), query)
