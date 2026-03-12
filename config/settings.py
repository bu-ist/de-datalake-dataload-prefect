"""
Application settings and configuration.
"""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    """
    # PostgreSQL Database
    postgres_host: str
    postgres_port: int = 5432
    postgres_db: str
    postgres_user: str
    postgres_pass: str

    # Campus Solutions Environment
    cs_env: str

    # PeopleSoft API
    people_soft_user: str
    people_soft_pass: str

    # SnapLogic APIs
    snaplogic_course_url: str
    snaplogic_course_key: str
    snaplogic_person_url: str
    snaplogic_person_key: str

    # VDS API
    vds_url: str
    vds_username: str
    vds_password: str

    # SAP API
    sap_url: str
    sap_key: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Global settings instance
settings = Settings()
