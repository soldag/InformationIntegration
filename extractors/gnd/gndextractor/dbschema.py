db_schema = {
    "persons": {
        "id": {
            "type": "varchar(16)",
            "primary_key": True,
            "null": False
        },
        "name": {
            "type": "varchar(256)"
        },
        "birth_name": {
            "type": "varchar(256)"
        },
        "life_span": {
            "type": "varchar(64)"
        },
        "place_of_birth": {
            "type": "varchar(16)",
            "reference": "places(id)"
        },
        "place_of_death": {
            "type": "varchar(16)",
            "reference": "places(id)"
        },
        "gender": {
            "type": "char"
        },
        "occupation": {
            "type": "varchar(256)"
        }
    },
    "person_relations": {
        "person1": {
            "type": "varchar(16)",
            "primary_key": True,
            "null": False,
            "reference": "persons(id)"
        },
        "person2": {
            "type": "varchar(16)",
            "primary_key": True,
            "null": False,
            "reference": "persons(id)"
        },
        "type": {
            "type": "varchar(256)",
            "primary_key": True,
            "null": False
        }
    },
    "works": {
        "id": {
            "type": "varchar(16)",
            "primary_key": True,
            "null": False
        },
        "name": {
            "type": "varchar(512)"
        }
    },
    "work_authors": {
        "work": {
            "type": "varchar(16)",
            "primary_key": True,
            "null": False,
            "reference": "works(id)"
        },
        "author": {
            "type": "varchar(16)",
            "primary_key": True,
            "null": False,
            "reference": "persons(id)"
        }
    },
    "places": {
        "id": {
            "type": "varchar(16)",
            "primary_key": True,
            "null": False
        },
        "name": {
            "type": "varchar(256)"
        },
        "latitude": {
            "type": "varchar(16)"
        },
        "longitude": {
            "type": "varchar(16)"
        }
    }
}