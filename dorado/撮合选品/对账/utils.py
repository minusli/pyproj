def field(*fields):
    for f in fields:
        if isinstance(f, (str,)):
            yield f
        elif isinstance(f, (tuple,)):
            assert len(f) == 2
            assert isinstance(f[0], (str,))
            assert isinstance(f[1], (str,))
            yield "{} as {}".format(f[0], f[1])
        else:
            raise Exception


def check_fields(*fields_list):
    if len(fields_list) == 0:
        return

    # 长度
    lens = [len(fields) for fields in fields_list]
    assert len(set(lens)) == 1, "len error"

    # 字段名
    field_idx = 0
    field_len = len(fields_list[0])
    while field_idx < field_len:
        fields = [get_field_name(fs[field_idx]) for fs in fields_list]
        assert len(set(fields)) == 1, "value error"
        field_idx += 1


def get_field_name(v: str | tuple) -> str:
    if isinstance(v, (tuple,)):
        return v[1]
    return v
