# tests/test_lambda_schema.py
import pytest
from lambda.extractor.schema import UserModel

def test_user_model_minimal():
    raw = {
        "gender": "male",
        "name": {"first": "John", "last": "Doe"},
        "email": "john@example.com",
        "location": {"city": "Bogota", "country": "CO"}
    }
    user = UserModel.parse_obj(raw)
    assert user.name.first == "John"
    assert user.name.last == "Doe"
    assert user.email == "john@example.com"
    assert user.location.city == "Bogota"
    assert user.location.country == "CO"
    # campos opcionales
    assert user.name.title is None
    assert user.dob is None
    assert user.login is None

def test_user_model_full():
    raw = {
        "gender": "male",
        "name": {"title": "Mr", "first": "Roland", "last": "Webb"},
        "location": {
            "street": {"number": 3553, "name": "The Drive"},
            "city": "Armagh",
            "state": "Cumbria",
            "country": "United Kingdom",
            "postcode": "QE5I 1AU",
            "coordinates": {"latitude": "-10.2453", "longitude": "-50.6278"},
            "timezone": {"offset": "-3:30", "description": "Newfoundland"}
        },
        "email": "roland.webb@example.com",
        "login": {"uuid": "df55d042-34b7-4e46-82b7-7d0b37af5a2e", "username": "sadkoala501"},
        "dob": {"date": "1991-02-02T04:05:31.963Z", "age": 34},
        "registered": {"date": "2003-03-31T11:44:24.906Z", "age": 22},
        "phone": "016977 79429",
        "cell": "07391 501024",
        "id": {"name": "NINO", "value": "GJ 83 37 49 H"},
        "picture": {
            "large": "https://randomuser.me/api/portraits/men/72.jpg",
            "medium": "https://randomuser.me/api/portraits/med/men/72.jpg",
            "thumbnail": "https://randomuser.me/api/portraits/thumb/men/72.jpg"
        },
        "nat": "GB"
    }
    user = UserModel.parse_obj(raw)
    assert user.name.first == "Roland"
    assert user.login.uuid == "df55d042-34b7-4e46-82b7-7d0b37af5a2e"
    assert user.picture.large.startswith("https://")
    assert user.dob.age == 34
    assert user.location.state == "Cumbria"
