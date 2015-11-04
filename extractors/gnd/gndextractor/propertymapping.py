property_mapping = {
    # Persons entities (Tpgesamt)
    "tp": {
        "persons": {
            ".": {
                "id": {
                    "xpath": "substring(ns:datafield[@tag='035']/ns:subfield[@code='a' and starts-with(./text(), '(DE-588)')], 9)",
                    "scope": "local"
                },
                "name": {
                    "xpath": "concat(ns:datafield[@tag='100']/ns:subfield[@code='a']/text(), ns:datafield[@tag='100']/ns:subfield[@code='b']/text())",
                    "scope": "local"
                },
                "birth_name": {
                    "xpath": "ns:datafield[@tag='400' and @ind1='1' and ns:subfield[@code='i']='Wirklicher Name']/ns:subfield[@code='a']/text()",
                    "scope": "local"
                },
                "life_span": {
                    "xpath": "ns:datafield[@tag='548' and ns:subfield[@code='i']='Exakte Lebensdaten']/ns:subfield[@code='a']/text()",
                    "scope": "local"
                },
                "place_of_birth": {
                    "xpath": "substring(ns:datafield[@tag='551' and ns:subfield[@code='i']='Geburtsort']/ns:subfield[@code='0' and starts-with(./text(), '(DE-588)')]/text(), 9)",
                    "scope": "local"
                },
                "place_of_death": {
                    "xpath": "substring(ns:datafield[@tag='551' and ns:subfield[@code='i']='Sterbeort']/ns:subfield[@code='0' and starts-with(./text(), '(DE-588)')]/text(), 9)",
                    "scope": "local"
                },
                "gender": {
                    "xpath": "ns:datafield[@tag='375']/ns:subfield[@code='a']/text()",
                    "scope": "local"
                },
                # TODO: n:m?
                "occupation": {
                    "xpath": "ns:datafield[@tag='550' and (ns:subfield[@code='i']='Charakteristischer Beruf' or ns:subfield[@code='i']='Beruf')]/ns:subfield[@code='a']/text()",
                    "scope": "local"
                }
            }
        },
        "person_relations": {
            "ns:datafield[@tag='500' and @ind1='1' and ns:subfield[@code='9']='4:bezf']": {
                "person1": {
                    "xpath": "substring(ns:subfield[@code='0' and starts-with(./text(), '(DE-588)')]/text(), 9)",
                    "scope": "local"
                },
                "person2": {
                    "xpath": "substring(ns:datafield[@tag='035']/ns:subfield[@code='a' and starts-with(./text(), '(DE-588)')], 9)",
                    "scope": "global"
                },
                "type": {
                    "xpath": "substring(ns:subfield[@code='9' and not(./text()='4:bezf')]/text(), 3)",
                    "scope": "local"
                }
            }
        },
    },
    # Literary works entities (Tugesamt)
    "tu": {
        "works": {
            ".": {
                "id": {
                    "xpath": "substring(ns:datafield[@tag='035']/ns:subfield[@code='a' and starts-with(./text(), '(DE-588)')]/text(), 9)",
                    "scope": "local"
                },
                "name": {
                    "xpath": "ns:datafield[@tag='100']/ns:subfield[@code='t']/text()",
                    "scope": "local"
                }
            }
        },
        "work_authors": {
            "//ns:record[ns:datafield[@tag='079']/ns:subfield[@code='b']='u']": {
                "work": {
                    "xpath": "substring(ns:datafield[@tag='035']/ns:subfield[@code='a' and starts-with(./text(), '(DE-588)')], 9)",
                    "scope": "global"
                },
                "author": {
                    "xpath": "substring(ns:datafield[@tag='500' and starts-with(ns:subfield[@code='i'], 'Verfasserschaft')]/ns:subfield[@code='0' and starts-with(./text(), '(DE-588)')]/text(), 9)",
                    "scope": "local"
                }
            }
        },
    },

    # Geographical entities (Tggesamt)
    "tg": {
        "places": {
            ".": {
                "id": {
                    "xpath": "substring(ns:datafield[@tag='035']/ns:subfield[@code='a' and starts-with(./text(), '(DE-588)')], 9)",
                    "scope": "local"
                },
                "name": {
                    "xpath": "ns:datafield[@tag='151']/ns:subfield[@code='a']/text()",
                    "scope": "local"
                },
                "latitude": {
                    "xpath": "ns:datafield[@tag='034' and ns:subfield[@code='9']='A:dgx']/ns:subfield[@code='f']/text()",
                    "scope": "local"
                },
                "longitude": {
                    "xpath": "ns:datafield[@tag='034' and ns:subfield[@code='9']='A:dgx']/ns:subfield[@code='d']/text()",
                    "scope": "local"
                }
            }
        }
    }
}
