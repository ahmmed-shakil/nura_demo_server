import logging
from fastapi import HTTPException
from sqlalchemy import create_engine, URL, Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, async_sessionmaker
from src.modules.config import settings


class Database:
    """Create and hold sync/async Engines and Sessionmakers instances."""

    _url = None
    _engine = None
    _session_factory = None
    _async_engine = None
    _async_session_factory = None

    @classmethod
    def get_url(cls) -> URL | None:
        return cls._url

    @classmethod
    def get_engine(cls, url: URL | None = None) -> Engine:
        if isinstance(url, URL):
            cls._url = url
            cls._session_engine = create_engine(
                cls._url, echo=False, connect_args={"options": "-c search_path=core,public"}
            )
        elif not cls._session_engine:
            raise ValueError("DB connection is not initiated yet, valid URL required")
        return cls._session_engine

    @classmethod
    def get_session(cls, url: URL | None = None) -> Session:
        return Session(cls.get_engine(url))

    @classmethod
    def get_session_factory(cls, url: URL | None = None) -> sessionmaker[Session]:
        if url or not cls._session_factory:
            cls._session_factory = sessionmaker(bind=cls.get_engine(url))
        return cls._session_factory

    @classmethod
    def get_async_engine(cls) -> AsyncEngine:
        if not cls._async_engine:
            cls._async_engine = create_async_engine(
                settings.DB_URL_ASYNC,
                echo=False,
                connect_args={"server_settings": {"search_path": "core,public"}},
            )
        return cls._async_engine

    @classmethod
    def get_async_session_factory(cls) -> async_sessionmaker:
        if not cls._async_session_factory:
            cls._async_session_factory = async_sessionmaker(bind=cls.get_async_engine())
        return cls._async_session_factory


credentials_exception = HTTPException(
    status_code=401,
    detail="User authorization failed",
    headers={"WWW-Authenticate": "Bearer"},
)


# Setup logging (console only, perishable logs)
def get_logger(name: str) -> logging.Logger:
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)
    log.addHandler(console)
    return log


formatter = logging.Formatter("%(name)-8s[%(levelname)-6s] %(message)s")
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(formatter)

logger = get_logger("backend")
auth_logger = get_logger("auth")
