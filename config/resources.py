"""
Resources for database and API connections.
"""
import base64
import asyncpg
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from typing import Dict
from config.settings import settings


class PostgresResource:
    """
    Resource for creating an asynchronous SQLAlchemy engine
    connected to a PostgreSQL database for insertion into the data lake.

    Attributes:
        host (str): The hostname of the PostgreSQL server.
        port (int): The port number on which the PostgreSQL server is listening.
        dbname (str): The name of the PostgreSQL database.
        user (str): The username for authenticating to PostgreSQL.
        password (str): The password for authenticating to PostgreSQL.
    """

    @staticmethod
    def get_engine() -> AsyncEngine:
        """
        Create and return an async SQLAlchemy engine.

        Returns:
            AsyncEngine: SQLAlchemy async engine for PostgreSQL
        """
        url = URL.create(
            drivername="postgresql+psycopg",
            username=settings.postgres_user,
            password=settings.postgres_pass,
            host=settings.postgres_host,
            port=settings.postgres_port,
            database=settings.postgres_db,
        )
        return create_async_engine(url, pool_pre_ping=True, future=True)


#TODO: Remove as we no longer use copy
class AsyncpgPoolResource:

    """
    Resource for creating an asynchronous asyncpg connection pool.
    This is different from PostgresResource because it uses asyncpg directly and allows COPY.

    Attributes:
        host (str): The hostname of the PostgreSQL server.
        port (int): The port number on which the PostgreSQL server is listening.
        dbname (str): The name of the PostgreSQL database.
        user (str): The username for authenticating to PostgreSQL.
        password (str): The password for authenticating to PostgreSQL.
    """

    @staticmethod
    def get_pool_config() -> Dict[str, any]:
        """
        Get asyncpg pool configuration.

        Returns:
            Dict containing asyncpg pool configuration
        """
        return {
            "host": settings.postgres_host,
            "port": settings.postgres_port,
            "database": settings.postgres_db,
            "user": settings.postgres_user,
            "password": settings.postgres_pass,
            "min_size": 12,
            "max_size": 24,
        }


class SnapLogicCourseApiResource:
    """
    Resource for storing SnapLogic API configuration and authentication
    details for course-related data extraction.

    Attributes:
        url (str): The base URL of the SnapLogic endpoint for course data.
        token (str): The API token for authenticating SnapLogic requests.
        cs_env (str): The Campus Solutions environment.
    """

    @staticmethod
    def get_config() -> Dict[str, any]:
        """
        Get SnapLogic Course API configuration.

        Returns:
            Dict containing URL, headers, and environment
        """
        return {
            "url": settings.snaplogic_course_url,
            "headers": {
                "x-api-key": settings.snaplogic_course_key,
                "User-Agent": "Mozilla/5.0"
            },
            "cs_env": settings.cs_env
        }


class DEPersonApiResource:
    """
    Resource for storing Data Engineering Person API configuration and authentication
    details for person-related data.

    Attributes:
        url (str): The base URL of the Data Engineering Person API endpoint.
        token (str): The API key for authenticating Data Engineering Person API requests.
        cs_env (str): The Campus Solutions environment.
    """

    @staticmethod
    def get_config() -> Dict[str, any]:
        """
        Get Data Engineering Person API configuration.

        Returns:
            Dict containing URL, headers, and environment
        """
        return {
            "url": settings.de_person_api_url,
            "headers": {
                "x-api-key": settings.de_person_api_key,
                "User-Agent": "Mozilla/5.0"
            },
            "cs_env": settings.cs_env
        }


class PsQueryResource:
    """
    Resource for connecting to the PeopleSoft API.

    Attributes:
        csEnv (str): The Campus Solutions environment.
        username (str): Username for basic authentication.
        password (str): Password for basic authentication.
    """

    @staticmethod
    def get_config() -> Dict[str, any]:
        """
        Get PeopleSoft API configuration.

        Returns:
            Dict containing CS environment and headers
        """
        auth_string = f"{settings.people_soft_user}:{settings.people_soft_pass}"
        encoded_auth = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')

        return {
            "csEnv": settings.cs_env,
            "headers": {
                "Authorization": f"Basic {encoded_auth}",
                "User-Agent": "Mozilla/5.0"
            }
        }


class VDSApiResource:
    """
    Resource for connecting to the VDS API.

    Attributes:
        url (str): URL endpoint for the VDS API.
        username (str): Username for basic authentication.
        password (str): Password for basic authentication.
    """

    @staticmethod
    def get_config() -> Dict[str, any]:
        """
        Get VDS API configuration.

        Returns:
            Dict containing URL and headers
        """
        auth_string = f"{settings.vds_username}:{settings.vds_password}"
        encoded_auth = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')

        return {
            "url": settings.vds_url,
            "headers": {
                "Authorization": f"Basic {encoded_auth}",
                "User-Agent": "Mozilla/5.0"
            }
        }


class SAPApiResource:
    """
    Resource for connecting to the SAP API.

    Attributes:
        url (str): The base URL of the SAP endpoint for employee data.
        token (str): The API key for authenticating SAP requests.
    """

    @staticmethod
    def get_config() -> Dict[str, any]:
        """
        Get SAP API configuration.

        Returns:
            Dict containing URL and headers
        """
        return {
            "url": settings.sap_url,
            "headers": {
                "x-api-key": settings.sap_key,
                "User-Agent": "Mozilla/5.0"
            }
        }


def ps_url(env: str, qry: str) -> str:
    """
    Helper function to construct PeopleSoft query URLs.

    Args:
        env: Campus Solutions environment
        qry: Query name

    Returns:
        Full URL for the PeopleSoft query
    """
    return f"https://cs{env}.bu.edu/PSIGW/RESTListeningConnector/PSFT_CS/ExecuteQuery.v1/PUBLIC/{qry}/JSON/NONFILE"


#TODO: Add "HOUSING_STAGE"."ETL_CURR_HOUSING_IDS"
