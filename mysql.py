from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine

LOCALHOST = "localhost"


def get_engine(auth:dict, tunnel: SSHTunnelForwarder) -> Engine:
    return create_engine(
        URL.create(
            drivername="mysql+pymysql",
            username=auth["MYSQL_USER"],
            password=auth["MYSQL_PWD"],
            host=LOCALHOST,
            port=tunnel.local_bind_port,
            database=auth["MYSQL_DB"],
        ),
    )


def get_data(engine: Engine, query: str) -> list:
    with engine.connect() as conn:
        results = conn.execute(query)
        return [dict(zip(results.keys(), result)) for result in results.fetchall()]


def get(auth: dict, query: str) -> list:
    with SSHTunnelForwarder(
        (auth["SSH_HOST"], int(auth["SSH_PORT"])),
        ssh_username=auth["SSH_USER"],
        ssh_password=auth["SSH_PWD"],
        remote_bind_address=(LOCALHOST, 3306),
    ) as tunnel:
        return get_data(get_engine(auth, tunnel), query)
