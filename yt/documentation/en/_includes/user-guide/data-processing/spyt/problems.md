
# Troubleshooting

## Cluster generates errors on startup

To get information on a running transaction, use the `spark-discovery-yt` utility:

```bash
spark-discovery-yt <cluster-name> --discovery-path my_discovery_path
```
A task list for this transaction always includes a master, a worker, and the Spark History Server. If the master is non-functional, so are the other components. If everything is running properly, the worker log will include data on the accepted task and the start of task execution.

If a task produces an error at startup, you need to:
- Open the task log via Master Web UI. It has links to the driver and the executor logs.
- Open Worker UI via Master Web UI and review the application startup sequence.
- View the [Event Log](logs.md).


