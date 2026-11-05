select * from (
    select
        Unwrap(cast(TableRow() as Struct<'date':Date,'nomenclature_id':Int64,'order_uuid':Uuid,'quantity':Int64>)),
        Unwrap(Yson::ConvertTo(order_meta, Struct<'order_collector': Int64, 'warehouse_rack': Int64, 'quality': List<Text>>))
    from self
) flatten columns
