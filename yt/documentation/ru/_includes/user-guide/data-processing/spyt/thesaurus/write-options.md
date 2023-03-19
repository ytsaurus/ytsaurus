
# Опции записи

## sorted_by

Указание сортировки по некоторому префиксу колонок:

```python
df.write.sorted_by("uuid").yt("//sys/spark/examples/test_data")
```

## unique_keys 

Уникальность ключа в таблице:

```python
df.write.sorted_by("uuid").unique_keys.yt("//sys/spark/examples/test_data")
```

## optimize_for

Таблица может храниться в построчном (lookup) и поколоночном (scan) формате. Предпочитаемый выбирается в зависимости от задачи:

```python
spark.write.optimize_for("scan").yt("//sys/spark/examples/test_data")
spark.write.optimize_for("lookup").yt("//sys/spark/examples/test_data")
```

