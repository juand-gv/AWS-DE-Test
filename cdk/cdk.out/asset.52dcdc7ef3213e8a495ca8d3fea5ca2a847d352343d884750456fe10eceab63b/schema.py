from pydantic import BaseModel, Field
from typing import Optional, Dict

class NameModel(BaseModel):
    title: Optional[str]
    first: Optional[str]
    last: Optional[str]

class StreetModel(BaseModel):
    number: Optional[int]
    name: Optional[str]

class CoordinatesModel(BaseModel):
    latitude: Optional[str]
    longitude: Optional[str]

class TimezoneModel(BaseModel):
    offset: Optional[str]
    description: Optional[str]

class LocationModel(BaseModel):
    street: Optional[StreetModel]
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]
    postcode: Optional[str]
    coordinates: Optional[CoordinatesModel]
    timezone: Optional[TimezoneModel]

class LoginModel(BaseModel):
    uuid: Optional[str]
    username: Optional[str]
    password: Optional[str]
    salt: Optional[str]
    md5: Optional[str]
    sha1: Optional[str]
    sha256: Optional[str]

class DobModel(BaseModel):
    date: Optional[str]
    age: Optional[int]

class RegisteredModel(BaseModel):
    date: Optional[str]
    age: Optional[int]

class IdModel(BaseModel):
    name: Optional[str]
    value: Optional[str]

class PictureModel(BaseModel):
    large: Optional[str]
    medium: Optional[str]
    thumbnail: Optional[str]

class UserModel(BaseModel):
    gender: Optional[str]
    name: Optional[NameModel]
    location: Optional[LocationModel]
    email: Optional[str]
    login: Optional[LoginModel]
    dob: Optional[DobModel]
    registered: Optional[RegisteredModel]
    phone: Optional[str]
    cell: Optional[str]
    id: Optional[IdModel]
    picture: Optional[PictureModel]
    nat: Optional[str]
