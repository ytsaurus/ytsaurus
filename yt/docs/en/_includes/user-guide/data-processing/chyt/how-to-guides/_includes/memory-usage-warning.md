{% note alert "Important" %}

Do not set the following options simultaneously:

- RAM size on the **Resources** tab in the _Instance Total Memory_ field.
- Memory allocation configuration on the **Advanced** tab in the _Instance memory_ field.

If you set both options at once, the controller cannot determine which one to apply. The clique will not start, and the system will output an error:

```
Failed to start
chyt: instance_memory and instance_total_memory cannot be specified simultaneously.
```

{% endnote %}
