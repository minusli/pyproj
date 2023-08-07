all_merge_fields = [
    "account_id",
    "app_id",
    "avail_qty",
    "biz_line",
    "category_code",
    "create_time",
    "create_uid",
    "has_common_commission",
    "hot_sold",
    "is_in_live",
    "is_in_poi",
    "online_status",
    "origin_price",
    "poi_list",
    "poi_num",
    "price",
    "product_commission_list",
    "product_id",
    "product_name",
    "product_type",
    "sold_count",
    "sold_end_time",
    "sold_l30_count",
    "sold_start_time",
    "tag_list",
    "take_rate",
    "task_id_list",
    "update_time",
    "es_source",
    "es_id",
    "es_seq_no",
    "es_op_type",
    "has_recruitment_task",
    "recruitment_task_list",
    "product_tag_list",
    "marketing_tag_list",
]
yesterday_merge_fields = [
    "account_id",
    "app_id",
    "avail_qty",
    "biz_line",
    "category_code",
    "create_time",
    "create_uid",
    "has_common_commission",
    "hot_sold",
    "is_in_live",
    "is_in_poi",
    "online_status",
    "origin_price",
    ("null", "poi_list"),
    "poi_num",
    "price",
    "product_commission_list",
    "product_id",
    "product_name",
    "product_type",
    "sold_count",
    "sold_end_time",
    "sold_l30_count",
    "sold_start_time",
    "tag_list",
    "take_rate",
    "task_id_list",
    "update_time",
    ("null", "es_source"),
    "es_id",
    "es_seq_no",
    "es_op_type",
    "has_recruitment_task",
    "recruitment_task_list",
    "product_tag_list",
    "marketing_tag_list",
]
today_fields = [
    ("get_json_object(source, '$.account_id')", "account_id"),
    ("get_json_object(source, '$.app_id')", "app_id"),
    ("get_json_object(source, '$.avail_qty')", "avail_qty"),
    ("get_json_object(source, '$.biz_line')", "biz_line"),
    ("get_json_object(source, '$.category_code')", "category_code"),
    ("get_json_object(source, '$.create_time')", "create_time"),
    ("get_json_object(source, '$.create_uid')", "create_uid"),
    ("get_json_object(source, '$.has_common_commission')", "has_common_commission"),
    ("get_json_object(source, '$.hot_sold')", "hot_sold"),
    ("get_json_object(source, '$.is_in_live')", "is_in_live"),
    ("get_json_object(source, '$.is_in_poi')", "is_in_poi"),
    ("get_json_object(source, '$.online_status')", "online_status"),
    ("get_json_object(source, '$.origin_price')", "origin_price"),
    ("null", "poi_list"),
    ("get_json_object(source, '$.poi_num')", "poi_num"),
    ("get_json_object(source, '$.price')", "price"),
    ("get_json_object(source, '$.product_commission_list')", "product_commission_list"),
    ("get_json_object(source, '$.product_id')", "product_id"),
    ("get_json_object(source, '$.product_name')", "product_name"),
    ("get_json_object(source, '$.product_type')", "product_type"),
    ("get_json_object(source, '$.sold_count')", "sold_count"),
    ("get_json_object(source, '$.sold_end_time')", "sold_end_time"),
    ("get_json_object(source, '$.sold_l30_count')", "sold_l30_count"),
    ("get_json_object(source, '$.sold_start_time')", "sold_start_time"),
    ("get_json_object(source, '$.tag_list')", "tag_list"),
    ("get_json_object(source, '$.take_rate')", "take_rate"),
    ("get_json_object(source, '$.task_id_list')", "task_id_list"),
    ("get_json_object(source, '$.update_time')", "update_time"),
    ("null", "es_source"),
    ("id", "es_id"),
    ("seq_no", "es_seq_no"),
    ("op_type", "es_op_type"),
    ("get_json_object(source, '$.has_recruitment_task')", "has_recruitment_task"),
    ("get_json_object(source, '$.recruitment_task_list')", "recruitment_task_list"),
    ("get_json_object(source, '$.product_tag_list')", "product_tag_list"),
    ("get_json_object(source, '$.marketing_tag_list')", "marketing_tag_list"),
]
today_merge_fields = [
    ("collect_set(account_id)[0]", "account_id"),
    ("collect_set(app_id)[0]", "app_id"),
    ("collect_set(avail_qty)[0]", "avail_qty"),
    ("collect_set(biz_line)[0]", "biz_line"),
    ("collect_set(category_code)[0]", "category_code"),
    ("collect_set(create_time)[0]", "create_time"),
    ("collect_set(create_uid)[0]", "create_uid"),
    ("collect_set(has_common_commission)[0]", "has_common_commission"),
    ("collect_set(hot_sold)[0]", "hot_sold"),
    ("collect_set(is_in_live)[0]", "is_in_live"),
    ("collect_set(is_in_poi)[0]", "is_in_poi"),
    ("collect_set(online_status)[0]", "online_status"),
    ("collect_set(origin_price)[0]", "origin_price"),
    (r"concat('[',concat_ws(',', collect_list(regexp_replace(poi_list, '(^\\[)|(\\]$)', ''))), ']')", "poi_list"),
    ("collect_set(poi_num)[0]", "poi_num"),
    ("collect_set(price)[0]", "price"),
    ("collect_set(product_commission_list)[0]", "product_commission_list"),
    ("collect_set(product_id)[0]", "product_id"),
    ("collect_set(product_name)[0]", "product_name"),
    ("collect_set(product_type)[0]", "product_type"),
    ("collect_set(sold_count)[0]", "sold_count"),
    ("collect_set(sold_end_time)[0]", "sold_end_time"),
    ("collect_set(sold_l30_count)[0]", "sold_l30_count"),
    ("collect_set(sold_start_time)[0]", "sold_start_time"),
    ("collect_set(tag_list)[0]", "tag_list"),
    ("collect_set(take_rate)[0]", "take_rate"),
    ("collect_set(task_id_list)[0]", "task_id_list"),
    ("collect_set(update_time)[0]", "update_time"),
    ("collect_set(es_source)[0]", "es_source"),
    ("collect_set(split(es_id, '-')[0])[0]", "es_id"),
    ("max(es_seq_no)", "es_seq_no"),
    ("IF(array_contains(collect_set(es_op_type), 'INDEX'), 'INDEX', 'DELETE')", "es_op_type"),
    ("collect_set(has_recruitment_task)[0]", "has_recruitment_task"),
    ("collect_set(recruitment_task_list)[0]", "recruitment_task_list"),
    ("collect_set(product_tag_list)[0]", "product_tag_list"),
    ("collect_set(marketing_tag_list)[0]", "marketing_tag_list"),
]


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


def check_fields():
    for i, f in enumerate(all_merge_fields):
        if isinstance(f, (tuple,)):
            f = f[1]

        f1 = yesterday_merge_fields[i]
        if isinstance(f1, (tuple,)):
            f1 = f1[1]
        f2 = today_fields[i]
        if isinstance(f2, (tuple,)):
            f2 = f2[1]
        f3 = today_merge_fields[i]
        if isinstance(f3, (tuple,)):
            f3 = f3[1]
        if not (f == f1 == f2 == f3):
            print(f, f1, f2, f3)
            raise Exception


if __name__ == '__main__':
    check_fields()
    index = "product_flat_buckets_forward_byte.es.alliance_goods_online_v2"
    stream_table = "life_alliance.ods_rocketmq_ebus_byte_es_poi_and_products"
    merge_table = "life_alliance.ods_rocketmq_ebus_byte_es_products_merge"
    today = "${date}"
    yesterday = "${date-1}"
    tomorrow = "${date+1}"
    limit = 0

    sql = f"""
    set spark.executor.memory=40g;
    
    with today_product as (
        select {",".join(field(*today_fields))}
        from {stream_table}
        where index='{index}'
        and date = '{today}'
        and CAST(get_json_object(source, '$.update_time') as BIGINT) < UNIX_TIMESTAMP('{tomorrow}', 'yyyyMMdd')
        {"limit {}".format(limit) if limit else ""}
    ), shift_product as (
        select {",".join(field(*today_fields))}
        from {stream_table}
        where index='{index}'
        and hour = '23'
        and date = '{yesterday}'
        and CAST(get_json_object(source, '$.update_time') as BIGINT) > UNIX_TIMESTAMP('{today}', 'yyyyMMdd')
        {"limit {}".format(limit) if limit else ""}
    ), latest_today_product as (
        select {",".join(field(*all_merge_fields))} 
        from (
            select *, row_number() over(partition by es_id order by es_seq_no desc) as rn
            from (
                select * from today_product union all select * from shift_product
            )
        ) 
        where rn=1
    ), today_merge_product as (
        select {",".join(field(*today_merge_fields))}
        from latest_today_product
        group by product_id
    ), yesterday_merge_product as (
        select {",".join(field(*yesterday_merge_fields))}
        from {merge_table}
        where index='{index}'
        and date='{yesterday}'
        {"limit {}".format(limit) if limit else ""}
    )
    insert overwrite table {merge_table} partition (date='{today}', index='{index}')
    select {",".join(field(*all_merge_fields))}
    from (
        select *, row_number() over(partition by es_id order by es_seq_no desc) as rn
        from (
            select * from today_merge_product union all select * from yesterday_merge_product
        )
    )
    where rn=1
    """

    print(sql)
