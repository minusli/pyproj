from libs import pygets


def concat(*args):
    return "".join(args)


def test_get():
    data = {
        "f1": "a",
        "f2": 2,
        "f3": {
            "f4": "b",
            "f5": 3,
            "f6": [
                "l1", "l2", "l3"
            ],
        },
        "f7": [
            {
                "f8": "f8-1",
                "f9": [1, 2, 3]
            },
            {
                "f8": "f8-2",
                "f9": [1, 2, 3]
            },
            {
                "f8": "f8-3",
                "f9": [1, 2, 3]
            }
        ],
        "f10": {
            "f11": [
                [{"f12": [1]}],
                [{"f12": [2]}],
                [{"f12": 3}],
            ]
        },
        "f13": '{"a": 1}',
        "f14": 0.01123,
        "f15": 0.01,
        "f16": 23.79
    }

    pygets.register_pipe("concat", concat)
    assert pygets.get(data, "f1") == "a"
    assert pygets.get(data, "f2") == 2
    assert pygets.get(data, "f2|str") == "2"
    assert pygets.get(data, "f3.f4") == "b"
    assert pygets.get(data, "f3.f5") == 3
    assert pygets.get(data, "f3.f5 | str  ") == "3"
    assert pygets.get(data, "f3.f6") == ["l1", "l2", "l3"]
    assert pygets.get(data, "f3.f6") == ["l1", "l2", "l3"]
    assert pygets.get(data, "f3.f6|jsondump") == '["l1", "l2", "l3"]'
    assert pygets.get(data, "f7.f8") == ["f8-1", "f8-2", "f8-3"]
    assert pygets.get(data, "f7.f9") == [[1, 2, 3], [1, 2, 3], [1, 2, 3]]
    assert pygets.get(data, "f10.f11.f12") == [[[1]], [[2]], [3]]
    assert pygets.get(data, "f13 | jsonload") == {"a": 1}
    assert pygets.get(data, "f14 | %") == "1.123%"
    assert pygets.get(data, "f14 | % 0") == "1%"
    assert pygets.get(data, "f14 | % 1") == "1.1%"
    assert pygets.get(data, "f14 | % 2") == "1.12%"
    assert pygets.get(data, "f14 | % 3") == "1.123%"
    assert pygets.get(data, "f14 | % 4") == "1.1230%"
    assert pygets.get(data, "f14 | % 5") == "1.12300%"
    assert pygets.get(data, "f15 | %") == "1%"
    assert pygets.get(data, "f16 | int") == 23
    assert pygets.get(data, "f16 | float") == 23.79
    assert pygets.get(data, "f16 | str | concat a b c") == "23.79abc"
