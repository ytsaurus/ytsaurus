{% note info %}

When started, the History Server indexes all records from the event logs source, and processing each record may take 10–15 seconds. New logs become available only after complete indexing of all existing data is finished.

In high-load clusters, this can lead to significant delays when starting or restarting the History Server. Therefore, for production clusters with an intense flow of tasks, it is recommended to use a new (empty) event logs table when launching a new Spark cluster. If necessary, the old table can be archived (move the table).

{% endnote %}
