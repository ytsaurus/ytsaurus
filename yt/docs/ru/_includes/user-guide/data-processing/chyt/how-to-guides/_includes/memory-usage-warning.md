{% note alert "Важно" %}

Не указывайте одновременно:

- объём памяти RAM на вкладке **Resources** в поле _Instance Total Memory_;
- конфигурацию распределения памяти на вкладке **Advanced** в поле _Instance memory_.

При одновременном указании обеих настроек контроллер не сможет определить, какую из них применить, клика не запустится, и система выдаст ошибку:

```
Failed to start
chyt: instance_memory and instance_total_memory cannot be specified simultaneously.
```

{% endnote %}
