select * from (
    select
        Unwrap(cast(TableRow() as Struct<'first_appeared': Timestamp,'id': Int64,'is_rx': Bool,'name': Text>)),
        Unwrap(Yson::ConvertTo(meta_data, Struct<'min_temperature': Int64, 'max_temperature': Int64, 'origin': Text>))
    from self
) flatten columns
