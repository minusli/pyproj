import inspect
from typing import Type, List, Dict, Any, TypeVar

Model = TypeVar("Model")
Row = Dict[str, Any]


class Result:
    def __init__(self, rows: List[Row], rowcount: int, insertid: Any):
        self._rows = rows
        self._rowcount = rowcount
        self._insertid = insertid

    def all(self, model: Type[Model] = None) -> List[Row | Model]:
        if not model:
            return self._rows
        return [fill_model(model(), **row) for row in self._rows]

    def first(self, model: Type[Model] = None) -> Row | Model:
        if not self._rows:
            return None
        if not model:
            return self._rows[0]
        return fill_model(model(), **self._rows[0])

    def rowcount(self) -> int:
        return self._rowcount

    def insertid(self) -> Any:
        return self._insertid


def fill_model(model: Model, **kwargs) -> Model:
    for k, v in kwargs.items():
        # 合理性判断：存在且非方法
        if not hasattr(model, k):
            continue

        if inspect.ismethod(getattr(model, k, None)):
            continue

        # 前置处理
        fn_name = "decode_{}".format(k)
        if hasattr(model, fn_name):
            fn = getattr(model, k)
            if inspect.ismethod(fn):
                v = fn(v)

        # field 注入
        setattr(model, k, v)

    return model
