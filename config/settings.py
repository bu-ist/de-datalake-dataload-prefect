from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    postgres_host: str
    postgres_port: int = 5432
    postgres_db: str
    postgres_user: str
    postgres_pass: SecretStr

    cs_env: str

    de_cstools_endpoint: str
    de_cstools_key: SecretStr

    snaplogic_course_url: str
    snaplogic_course_key: SecretStr

    de_person_api_url: str
    de_person_api_key: SecretStr

    vds_url: str
    vds_key: SecretStr

    sap_url: str
    sap_key: SecretStr


settings = Settings()
