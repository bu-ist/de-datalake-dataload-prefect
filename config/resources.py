import asyncpg
import base64
from typing import Dict
from config.settings import settings


class PostgresResource:
    @staticmethod
    async def get_pool(min_size: int = 12, max_size: int = 24) -> asyncpg.Pool:
        return await asyncpg.create_pool(
            host=settings.postgres_host,
            port=settings.postgres_port,
            database=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_pass,
            min_size=min_size,
            max_size=max_size,
        )


class SnapLogicCourseApiResource:
    @staticmethod
    def get_config() -> Dict[str, any]:
        return {
            "url": settings.snaplogic_course_url,
            "headers": {
                "x-api-key": settings.snaplogic_course_key,
                "User-Agent": "Mozilla/5.0"
            },
            "cs_env": settings.cs_env
        }


class DEPersonApiResource:
    @staticmethod
    def get_config() -> Dict[str, any]:
        return {
            "url": settings.de_person_api_url,
            "headers": {
                "x-api-key": settings.de_person_api_key,
                "User-Agent": "Mozilla/5.0"
            },
            "cs_env": settings.cs_env
        }


class CsToolsResource:
    @staticmethod
    def get_config() -> Dict[str, any]:
        return {
            "url": settings.de_cstools_endpoint,
            "headers": {
                "x-api-key": settings.de_cstools_key,
                "User-Agent": "Mozilla/5.0",
                "Content-Type": "application/json"
            }
        }


class VDSApiResource:
    @staticmethod
    def get_config() -> Dict[str, any]:
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
    @staticmethod
    def get_config() -> Dict[str, any]:
        return {
            "url": settings.sap_url,
            "headers": {
                "x-api-key": settings.sap_key,
                "User-Agent": "Mozilla/5.0"
            }
        }


#TODO: Add "HOUSING_STAGE"."ETL_CURR_HOUSING_IDS"
