from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    postgres_host: str
    postgres_port: int = 5432
    postgres_db: str
    postgres_user: str
    postgres_pass: str

    cs_env: str

    de_cstools_endpoint: str
    de_cstools_key: str

    snaplogic_course_url: str
    snaplogic_course_key: str

    de_person_api_url: str
    de_person_api_key: str

    vds_url: str
    vds_key: str

    sap_url: str
    sap_key: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


settings = Settings()
