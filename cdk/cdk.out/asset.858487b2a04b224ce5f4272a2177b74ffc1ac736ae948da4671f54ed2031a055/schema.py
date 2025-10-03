from pydantic import BaseModel
from typing import Optional

class NameModel(BaseModel):
    title: Optional[str]
    first: str
    last: str

class LocationModel(BaseModel):
    city: Optional[str]
    country: Optional[str]

class UserModel(BaseModel):
    gender: Optional[str]
    name: NameModel
    email: str
    location: LocationModel