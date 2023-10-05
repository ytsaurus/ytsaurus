# Reading and writing within a transaction

{% if audience == "public" %}

For more information on transactions, please see the [Transactions](../../../../user-guide/dynamic-tables/transactions.md) section.

{% endif %}

By default, Spark does not create transactions for reading or take out any locks. To assure reading consistency for changing tables, you can create your own transactions and redirect them to read a Spark DataFrame.

Python example:

```python
with yt.Transaction() as tr:
    df = spark.read.option("transaction", tr.transaction_id).yt("//sys/spark/examples/example_1")
    df.show()
```

At the time of the call to `spark.read.yt`, a lock is taken out on the table (snapshot lock): while a plan is being compiled and until the physical read begins. In the example provided, the two calls to `show()` will read the table twice, necessarily returning the same result both times.

```python
with yt.Transaction() as tr:
    df = spark.read.option("transaction", tr.transaction_id).yt("//sys/spark/examples/example_1")
    df.show()
    time.sleep(60)
    df.show()
```

{% note info "Note" %}

You can enable creation of a global transaction that will open at the top of a Spark session and remain open the entire time your job is running. When using a global transaction, you have to keep in mind that the job will take out locks on all the tables it reads and maintain them until it exits. You enable a global transaction by setting configuration parameter `spark.yt.globalTransaction.enabled=true`.

{% endnote %}

When you need to write a DataFrame within a transaction you should use `write_transaction` option:

```python
df.write.option('write_transaction', transaction_id).yt("//target/table/path")
```
