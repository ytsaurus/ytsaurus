def batching_write_rows(records, writer, batch_size=100):
    for i in range(0, len(records), batch_size):
        writer(records[i : i + batch_size])
