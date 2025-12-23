from datetime import datetime, timedelta, timezone
from typing import Annotated
from uuid import uuid4
import bcrypt
from jose import JWTError, jwt, ExpiredSignatureError
from fastapi import APIRouter, HTTPException, Response, Depends, Query
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from src.modules import Database, auth_logger, credentials_exception
from src.modules.config import settings
from src.modules.model import User, Role, RefreshTokenDB
from src.modules.schemas.auth import UserOut, RefreshToken, RefreshTokenEncoded


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")
router = APIRouter()


def authenticate_user(email: str, password: str) -> User | None:
    with Database.get_session() as session:
        user = (
            session.query(
                User.id,
                User.customer_id,
                User.email,
                User.role_id,
                User.full_name,
                User.image,
                User.is_active,
                User.password,
                Role.name.label("role_name"),
                Role.description.label("role_description"),
            )
            .join(User.role)
            .filter(User.email == email)
            .first()
        )
    if not user:
        auth_logger.warning("Unknown user: %s", email)
        return None
    elif not user.is_active:
        auth_logger.warning("User is disabled: %s", email)
        return None
    elif not verify_password(password, user.password):
        auth_logger.warning("Wrong password provided for user: %s", email)
        return None
    return user


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(
        plain_password.encode("utf-8"), hashed_password.encode("utf-8")
    )


def create_access_token(user_id: int, role_id: int) -> str:
    auth_logger.debug("Create access token for user id: %s", user_id)
    expire = datetime.now(timezone.utc) + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    return create_token(user_id, role_id, expire)


def create_token(user_id: int, role_id: int, expire: datetime, tid: str | None = None) -> str:
    to_encode = {"sub": str(user_id), "exp": expire, "role": role_id}
    if tid:
        to_encode["tid"] = tid
    auth_logger.debug("to_encode %s", to_encode)
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, settings.ALGORITHM)
    auth_logger.debug("JWT encoded.")
    return encoded_jwt


def validate_access_token(token: Annotated[str, Depends(oauth2_scheme)]):
    auth_logger.debug("Validating access token content")
    validate_token(token)
    return


def validate_token(token: str) -> dict:
    auth_logger.debug("Decoding token")
    try:
        token_decoded = jwt.decode(token, settings.SECRET_KEY, settings.ALGORITHM)
        auth_logger.debug("Token content: %s", token_decoded)
        # TODO: implement role validation logic
        user_id = token_decoded.get("sub")
        role = token_decoded.get("role")
        if not role:
            auth_logger.warning("User is not authorized. User ID: %s", user_id)
            raise credentials_exception
    except ExpiredSignatureError as err:
        auth_logger.error("Expired token: %s\nError: %s", token, err)
        raise credentials_exception from err
    except JWTError as err:
        auth_logger.error("Invalid token: %s\nError: %s", token, err)
        raise credentials_exception from err
    except Exception as err:
        auth_logger.error("Token failure: %s\n Error: %s", token, err)
        raise credentials_exception from err
    auth_logger.debug("Token content is valid")
    return token_decoded

def validate_refresh_token(refresh_token: str) -> RefreshToken:
    auth_logger.debug("Starting validation for refresh token: %s", refresh_token)
    try:
        # Validate the token structure
        auth_logger.debug("Decoding refresh token...")
        token_decoded = validate_token(refresh_token)
        auth_logger.debug("Decoded token: %s", token_decoded)

        # Validate the token's contents
        auth_logger.debug("Validating token structure against RefreshToken model... ")
        old_refresh_token = RefreshToken.model_validate(token_decoded)
    except Exception as err:
        auth_logger.error(
            "Refresh token mapping failure: %s\n Error: %s", refresh_token, err
        )
        # Log the type of the error for better debugging
        auth_logger.debug("Error type: %s", type(err).__name__)
        raise credentials_exception from err

    auth_logger.debug("Successfully validated refresh token: %s", old_refresh_token)
    return old_refresh_token


def create_refresh_token(user_id: int, role_id: int) -> RefreshTokenEncoded:
    uuid = str(uuid4())
    expire = datetime.now(timezone.utc) + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    token = create_token(user_id, role_id, expire, uuid)
    return RefreshTokenEncoded(
        token_id=uuid,
        user_id=user_id,
        role_id=role_id,
        expire=expire,
        token=token,
    )


def replace_refresh_token(
    new_token: RefreshTokenEncoded, old_token: RefreshToken | None = None
):
    auth_logger.debug("Replacing refresh token.")
    try:
        with Database.get_session() as session:
            if old_token:
                db_token = (
                    session.query(RefreshTokenDB)
                    .filter(
                        RefreshTokenDB.token_id == old_token.token_id,
                        RefreshTokenDB.user_id == old_token.user_id,
                        RefreshTokenDB.expire > datetime.now(timezone.utc),
                        RefreshTokenDB.is_active == True,
                    )
                    .first()
                )
                if not db_token:
                    auth_logger.error(
                        "Refresh token %s not found in DB", old_token.token_id
                    )
                    raise credentials_exception
                db_token.is_active = False
            session.add(
                RefreshTokenDB(**new_token.model_dump(exclude={"role_id", "token"}))
            )
            session.commit()
    except Exception as err:
        session.rollback()
        auth_logger.error("Failed to replace refresh token. Error: %s", err)
        raise err
    return


@router.post("/", response_model=UserOut)
async def login_user(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()], response: Response
):
    auth_logger.info("User authentication: %s", form_data.username)
    try:
        user = authenticate_user(form_data.username, form_data.password)
    except Exception as err:
        auth_logger.error(
            "Internal auth error for: %s\nError: %s", form_data.username, err
        )
        raise HTTPException(
            status_code=500,
            detail="Authorization error",
        ) from err
    if not user:
        raise HTTPException(
            status_code=401,
            detail="User authorization failed",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(user.id, user.role_id)
    refresh_token = create_refresh_token(user.id, user.role_id)
    replace_refresh_token(refresh_token)
    response.headers["Authorization"] = f"Bearer {access_token}"
    response.headers["X-Refresh-Token"] = f"{refresh_token.token}"
    response.set_cookie(
        key="refresh_token",
        value=refresh_token.token,
        max_age=7 * 24 * 60 * 60,
        path="login/refresh-token",
        secure=True,
        httponly=True,
        samesite=None,
    )
    auth_logger.info("Authentication success for user: %s", form_data.username)
    return user


@router.post("/refresh-token")
async def refresh(
    refresh_token: Annotated[str, Query(),],
    response: Response,
    # refresh_token: Annotated[str | None, Cookie()] = None,
):
    auth_logger.info("refresh_token: %s", refresh_token)
    # if not refresh_token:
    #     auth_logger.error("Refresh token not provided")
    #     raise credentials_exception
    auth_logger.info("Refreshing token")
    old_refresh_token = validate_refresh_token(refresh_token)
    access_token = create_access_token(old_refresh_token.user_id, old_refresh_token.role_id)
    new_refresh_token = create_refresh_token(old_refresh_token.user_id, old_refresh_token.role_id)
    replace_refresh_token(new_refresh_token, old_refresh_token)
    response.headers["Authorization"] = f"Bearer {access_token}"
    response.headers["X-Refresh-Token"] = f"{new_refresh_token.token}"
    # response.set_cookie(
    #     key="refresh_token",
    #     value=new_refresh_token.token,
    #     max_age=7 * 24 * 60 * 60,
    #     path="login/refresh-token",
    #     secure=True,
    #     httponly=True,
    #     samesite=None,
    # )
    auth_logger.info(
        "Token successfully refreshed for user: %s", new_refresh_token.user_id
    )
    return
