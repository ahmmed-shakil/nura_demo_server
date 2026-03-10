from time import sleep
from sqlalchemy import URL, exc, text,create_engine
from sqlalchemy.orm import sessionmaker
from src.modules import Database, logger
from src.modules.config import settings
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine


url = URL.create(
            settings.DB_CONNECTION,
            settings.DB_USERNAME,
            settings.DB_PASSWORD,
            settings.DB_HOST,
            settings.DB_PORT,
            settings.DB_DATABASE,
        )
print(url)
engines = create_engine(url, echo=True, connect_args={"options": "-c search_path=core,public"})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engines)

def db_init(counter: int = 1) -> bool:
    """
    Initiate DB connection
    """
    try:
        url = URL.create(
            settings.DB_CONNECTION,
            settings.DB_USERNAME,
            settings.DB_PASSWORD,
            settings.DB_HOST,
            settings.DB_PORT,
            settings.DB_DATABASE,
        )
    except exc.ArgumentError as e:
        logger.error("Wrong DB connection argument. Error: %s", str(e))
        return False

    logger.info("DB connection: %s", str(url))
    # create and check DB Session Factory
    while counter > 0:
        if test_db(url):
            return True
        sleep(10)
        counter -= 1
    return False


def test_db(url: URL | None = None) -> bool:
    """
    Test DB connection
    """

    try:
        Session = Database.get_session_factory(url)
        with Session() as test_session:
            result = test_session.execute(text("select 1")).fetchone()
            if result:
                logger.info(
                    "DB connection established. URL: %s", str(url)
                )
                return True
    except Exception as e:
        logger.error(
            "DB connection error. URL: %s\n Error: %s", str(url), e
        )
    return False
