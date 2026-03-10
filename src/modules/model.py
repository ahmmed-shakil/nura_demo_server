from datetime import datetime
from sqlalchemy import ForeignKey, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from src.modules import Database
from src.modules.config import settings




# declarative base class
class Base(DeclarativeBase):
    pass

class ImageCaptureFromCCTV(Base):
    __tablename__ = 'image_capture_from_cctv'
    id: Mapped[int] = mapped_column(primary_key=True)
    device_serial: Mapped[str] = mapped_column(nullable=False)
    image: Mapped[str]
    create_datetime: Mapped[datetime] = mapped_column(
        server_default=func.timezone("UTC", func.current_timestamp())
    )


class Customer(Base):
    __tablename__ = "customer"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    description: Mapped[str | None]
    create_date: Mapped[datetime] = mapped_column(
        server_default=func.timezone("UTC", func.current_timestamp())
    )
    logo: Mapped[str | None]


class User(Base):
    __tablename__ = "user"

    id: Mapped[int] = mapped_column(primary_key=True)
    customer_id: Mapped[int] = mapped_column(ForeignKey("customer.id"), nullable=False)
    email: Mapped[str] = mapped_column(nullable=False)
    password: Mapped[str] = mapped_column(nullable=False)
    role_id: Mapped[int] = mapped_column(ForeignKey("role.id"), nullable=False)
    full_name: Mapped[str]
    image: Mapped[str]
    is_active: Mapped[bool] = mapped_column(server_default="True")
    create_datetime: Mapped[datetime] = mapped_column(
        server_default=func.timezone("UTC", func.current_timestamp())
    )

    role: Mapped["Role"] = relationship(back_populates="users")

    def __repr__(self) -> str:
        return f"""user(id={self.id!r}, customer_id={self.customer_id!r}, email={self.email!r},
password={self.password!r}, role_id={self.role_id!r}, full_name={self.full_name!r},
image={self.image!r}, is_active={self.is_active!r}, create_datetime={self.create_datetime!r})"""


class Role(Base):
    __tablename__ = "role"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    description: Mapped[str]
    is_active: Mapped[bool] = mapped_column(server_default="True")
    create_datetime: Mapped[datetime] = mapped_column(
        server_default=func.timezone("UTC", func.current_timestamp())
    )

    users: Mapped[list["User"]] = relationship(back_populates="role")

    def __repr__(self) -> str:
        return f"""user(id={self.id!r}, name={self.name!r}, description={self.description!r},
is_active={self.is_active!r}, create_datetime={self.create_datetime!r})"""


class RefreshTokenDB(Base):
    __tablename__ = "refresh_token"

    id: Mapped[int] = mapped_column(primary_key=True)
    token_id: Mapped[str] = mapped_column(nullable=False)
    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"), nullable=False)
    expire: Mapped[datetime] = mapped_column(nullable=False)
    is_active: Mapped[bool] = mapped_column(server_default="True")
    create_datetime: Mapped[datetime] = mapped_column(
        server_default=func.timezone("UTC", func.current_timestamp())
    )

    def __repr__(self) -> str:
        return f"""RefreshToken(id={self.id!r}, token_id={self.token_id!r}, user_id={self.user_id!r},
expire={self.expire!r}, is_active={self.is_active!r}, create_datetime={self.create_datetime!r})"""





# print(engine)
# # Base.metadata.create_all(bind=engine)