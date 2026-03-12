import dagster as dg
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine
import base64
import asyncpg

class PostgresResource(dg.ConfigurableResource):
    """
    Dagster resource for creating an asynchronous SQLAlchemy engine
    connected to a PostgreSQL database for insertion into the data lake.

    Attributes:
        host (str): The hostname of the PostgreSQL server.
        port (int): The port number on which the PostgreSQL server is listening.
        dbname (str): The name of the PostgreSQL database.
        user (str): The username for authenticating to PostgreSQL.
        password (str): The password for authenticating to PostgreSQL.
    """

    host: str = dg.EnvVar("POSTGRES_HOST")
    port: int = dg.EnvVar("POSTGRES_PORT")
    dbname: str = dg.EnvVar("POSTGRES_DB")
    user: str = dg.EnvVar("POSTGRES_USER")
    password: str = dg.EnvVar("POSTGRES_PASS")

    def create_resource(self, context):
        url = URL.create(
            drivername="postgresql+psycopg",
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.dbname,
        )
        return create_async_engine(url, pool_pre_ping=True, future=True)

#TODO: Remove as we no longer use copy
class AsyncpgPoolResource(dg.ConfigurableResource):

    """
    Dagster resource for creating an asynchronous SQLAlchemy engine.
    This is different from PostgresResource because it uses asyncpg directly and allows COPY.

    Attributes:
        host (str): The hostname of the PostgreSQL server.
        port (int): The port number on which the PostgreSQL server is listening.
        dbname (str): The name of the PostgreSQL database.
        user (str): The username for authenticating to PostgreSQL.
        password (str): The password for authenticating to PostgreSQL.
    """

    host: str = dg.EnvVar("POSTGRES_HOST")
    port: int = dg.EnvVar("POSTGRES_PORT")
    dbname: str = dg.EnvVar("POSTGRES_DB")
    user: str = dg.EnvVar("POSTGRES_USER")
    password: str = dg.EnvVar("POSTGRES_PASS")

    min_size: int = 12
    max_size: int = 24

    def create_resource(self, context):
        return {
            "host": self.host,
            "port": self.port,
            "database": self.dbname,
            "user": self.user,
            "password": self.password,
            "min_size": self.min_size,
            "max_size": self.max_size,
        }


class SnapLogicCourseApiResource(dg.ConfigurableResource):
    """
    Dagster resource for storing SnapLogic API configuration and authentication
    details for course-related data extraction.

    Attributes:
        url (str): The base URL of the SnapLogic endpoint for course data.
        token (str): The API token for authenticating SnapLogic requests.
        cs_env (str): The Campus Solutions environment.
    """

    url: str = dg.EnvVar("SNAPLOGIC_COURSE_URL")
    token: str = dg.EnvVar("SNAPLOGIC_COURSE_KEY")
    cs_env: str = dg.EnvVar("CS_ENV")

    def create_resource(self, context):
        return {
            "url": self.url,
            "headers": {
                "x-api-key": self.token,
                "User-Agent": "Mozilla/5.0"
            },
            "cs_env": self.cs_env
        }


class SnapLogicPersonApiResource(dg.ConfigurableResource):
    """
    Dagster resource for storing SnapLogic API configuration and authentication
    details for person-related data.

    Attributes:
        url (str): The base URL of the SnapLogic endpoint for person data.
        token (str): The API key for authenticating SnapLogic requests.
        cs_env (str): The Campus Solutions environment.
    """

    url: str = dg.EnvVar("SNAPLOGIC_PERSON_URL")
    token: str = dg.EnvVar("SNAPLOGIC_PERSON_KEY")
    cs_env: str = dg.EnvVar("CS_ENV")

    def create_resource(self, context):
        return {
            "url": self.url,
            "headers": {
                "x-api-key": self.token,
                "User-Agent": "Mozilla/5.0"
            },
            "cs_env": self.cs_env
        }

class PsQueryResource(dg.ConfigurableResource):
    """
    Dagster resource for connecting to the PeopleSoft API. 

    Attributes:
        csEnv (str): The Campus Solutions environment.
        username (str): Username for basic authentication.
        password (str): Password for basic authentication.
    """

    csEnv: str = dg.EnvVar("CS_ENV")
    username: str = dg.EnvVar("PEOPLE_SOFT_USER")
    password: str = dg.EnvVar("PEOPLE_SOFT_PASS")

    def create_resource(self, context):
        return {
            "csEnv": self.csEnv,
            "headers": {
                "Authorization": f"Basic {base64.b64encode(f'{self.username}:{self.password}'.encode('utf-8')).decode('utf-8')}",
                "User-Agent": "Mozilla/5.0"
            }
        }

class BUTermQueryResource(dg.ConfigurableResource):
    """
    Dagster resource for connecting to the PeopleSoft BU Term Query API BU_TERM_QRY.

    Attributes:
        url (str): URL endpoint for the BU term query.
        username (str): Username for basic authentication.
        password (str): Password for basic authentication.
    """

    url: str = dg.EnvVar("BU_TERM_QRY_URL")
    username: str = dg.EnvVar("PEOPLE_SOFT_USER")
    password: str = dg.EnvVar("PEOPLE_SOFT_PASS")

    def create_resource(self, context):
        return {
            "url": self.url,
            "headers": {
                "Authorization": f"Basic {base64.b64encode(f'{self.username}:{self.password}'.encode('utf-8')).decode('utf-8')}",
                "User-Agent": "Mozilla/5.0"
            }
        }

class VDSApiResource(dg.ConfigurableResource):
    """
    Dagster resource for connecting to the VDS API.

    Attributes:
        url (str): URL endpoint for the VDS API.
        username (str): Username for basic authentication.
        password (str): Password for basic authentication.
    """

    url: str = dg.EnvVar("VDS_URL")
    username: str = dg.EnvVar("VDS_USERNAME")
    password: str = dg.EnvVar("VDS_PASSWORD")

    def create_resource(self, context):
        return {
            "url": self.url,
            "headers": {
                "Authorization": f"Basic {base64.b64encode(f'{self.username}:{self.password}'.encode('utf-8')).decode('utf-8')}",
                "User-Agent": "Mozilla/5.0"
            }
        }


class SAPApiResource(dg.ConfigurableResource):
    """
    Dagster resource for connecting to the SAP API.

    Attributes:
        url (str): The base URL of the SAP endpoint for employee data.
        token (str): The API key for authenticating SAP requests.
        cs_env (str): The Campus Solutions environment.
    """

    url: str = dg.EnvVar("SAP_URL")
    token: str = dg.EnvVar("SAP_KEY")

    def create_resource(self, context):
        return {
            "url": self.url,
            "headers": {
                "x-api-key": self.token,
                "User-Agent": "Mozilla/5.0"
            }
        }

#TODO: Add "HOUSING_STAGE"."ETL_CURR_HOUSING_IDS"