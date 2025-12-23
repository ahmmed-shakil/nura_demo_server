from datetime import datetime
from typing import Optional
from pydantic import BaseModel, PositiveInt, Field, AliasChoices


class UserOut(BaseModel):
    id: PositiveInt
    customer_id: PositiveInt
    email: str
    role_id: PositiveInt
    full_name: str
    image: Optional[str]
    is_active: bool
    role_name: str
    role_description: Optional[str]


class RefreshToken(BaseModel):
    token_id: str = Field(validation_alias=AliasChoices("token_id", "tid"))
    user_id: PositiveInt = Field(validation_alias=AliasChoices("user_id", "sub"))
    expire: datetime = Field(validation_alias=AliasChoices("expire", "exp"))
    role_id: PositiveInt = Field(validation_alias=AliasChoices("role_id", "role"))


class RefreshTokenEncoded(RefreshToken):
    token: str
