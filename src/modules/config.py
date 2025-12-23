import os
from typing import Any, Annotated
from dotenv import dotenv_values
from pydantic import BaseModel, ValidationInfo, field_validator, AnyHttpUrl, Field


config = {
    **dotenv_values(".env"),  # load default variables
    **dotenv_values(".env.local"),  # load default variables
    **os.environ,  # override loaded values with environment variables
}


class Settings(BaseModel):
    # CORS
    ORIGINS: Annotated[list[AnyHttpUrl] | list[str], Field(validate_default=True)] = (
        config.get("ORIGINS", ["*"])
    )

    @field_validator("ORIGINS", mode="before")
    @classmethod
    def assemble_cors_origins(cls, v: str | list[str]) -> list[str]:
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, list):
            return v
        raise ValueError(v)

    # Auth
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(config.get("ACCESS_TOKEN_EXPIRE_MINUTES", 30))
    REFRESH_TOKEN_EXPIRE_DAYS: int = int(config.get("REFRESH_TOKEN_EXPIRE_DAYS", 7))
    ALGORITHM: str = str(config.get("ALGORITHM", "HS256"))
    SECRET_KEY: Annotated[str, Field(validate_default=True)] = str(config.get("SECRET_KEY"))
    # Database
    DB_HOST: Annotated[str, Field(validate_default=True)] = str(config.get("DB_HOST"))
    DB_PORT: int = int(config.get("DB_PORT", 5432))
    DB_CONNECTION: str = config.get("DB_CONNECTION", "postgresql+psycopg")
    DB_USERNAME: Annotated[str, Field(validate_default=True)] = str(config.get("DB_USERNAME"))
    DB_PASSWORD: Annotated[str, Field(validate_default=True)] = str(config.get("DB_PASSWORD"))
    DB_DATABASE: Annotated[str, Field(validate_default=True)] = str(config.get("DB_DATABASE"))
    # Report settings
    OUTPUT_PATH: str = str(config.get("OUTPUT_PATH", "./src/reports/output"))
    TEMPLATE_PATH: str = str(config.get("TEMPLATE_PATH", "./src/reports/templates"))
    TEMP_PATH: str = str(config.get("TEMP_PATH", "./src/temp"))
    DAILY_REPORT: str = str(config.get("DAILY_REPORT", "daily_report.xlsx"))
    # AWS S3
    AWS_ACCESS_KEY_ID: str = str(config.get("AWS_ACCESS_KEY_ID"))
    AWS_SECRET_ACCESS_KEY: str = str(config.get("AWS_SECRET_ACCESS_KEY"))
    BUCKET_NAME: str = str(config.get("BUCKET_NAME"))
    REGION_NAME: str = str(config.get("REGION_NAME", "eu-north-1"))
    MEDIA_FILE_PATH: str = str(config.get("MEDIA_FILE_PATH", "media"))
    FILE_PATH: str = str(config.get("FILE_PATH"))

    @property
    def DB_URL_SYNC(self):
        return f"postgresql+psycopg://{self.DB_USERNAME}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_DATABASE}"

    @property
    def DB_URL_ASYNC(self):
        return f"postgresql+asyncpg://{self.DB_USERNAME}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_DATABASE}"

    # fields validation
    @field_validator(
        "SECRET_KEY", "DB_HOST", "DB_USERNAME", "DB_PASSWORD", "DB_DATABASE",
        mode="before",
    )
    @classmethod
    def validate_field(cls, v: Any, info: ValidationInfo) -> str:
        if not isinstance(v, str) or v in ["None", ""]:
            raise ValueError(f"{info.field_name} is not valid", v)
        return v


settings = Settings()
