import random
from faker import Faker


def age():
    return random.randint(12, 99)


def get_text_fakers(column_name: str, faker: Faker):
    def gender():
        return random.choice(["male", "female", "non binary"])

    func_map = [
        (["first name"], "first_name"),
        (["last name"], "last_name"),
        (["name"], "name"),
        (["street"], "street_address"),
        (["address"], "address"),
        (["country code"], "country_code"),
        (["country"], "country"),
        (["city"], "city"),
        (["company"], "company"),
        (["credit card", "card", "card number"], "credit_card_number"),
        (["account", "bank account"], "iban"),
        (["e mail", "email"], "email"),
        (["currency", "cy"], "currency"),
        (["bank"], "bank"),
        (["phone", "mobile", "fax"], "phone_number"),
        (["date of birth", "birth date"], "date_of_birth"),
        (["ip"], "ipv6"),
        (["color"], "color"),
        (["host"], "hostname"),
        (["lang", "lanaguage"], "lanaguage_name"),
        (["date"], "date"),
        (["timestamp"], "timestamp"),
        (["gender"], gender),
        (["age"], age),
    ]
    func = None
    for keywords, ff in func_map:
        if any(kywrd in column_name for kywrd in keywords):
            func = ff
            break
    if isinstance(func, str) or func is None:
        func = getattr(faker, func or "pystr")
    return str(f'"{func()}"')


def get_fake_value_by_spark_type(typename: str, faker: Faker):
    func_map = {
        "DecimalType": "pydecimal",
        "DoubleType": "pyint",
        "FloatType": "pyfloat",
        "IntegerType": "pyint",
        "LongType": "pyint",
        "NumericType": "pyint",
        "ShortType": "pyint",
        "DateType": "date_object",
    }

    fnc_name = func_map.get(typename, "pyint")
    fnc = getattr(faker, fnc_name)
    return fnc()
