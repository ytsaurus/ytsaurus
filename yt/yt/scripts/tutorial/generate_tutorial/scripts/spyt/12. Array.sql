-- Example of working with an array of JSON objects
SELECT
  x.id, x.name -- Selecting the id and name fields from each element in the array
FROM (
  SELECT from_json(json_array_col, 'ARRAY<STRUCT<id:INT, name:STRING>>') as arr -- Parsing the JSON array string into an array of structs
  FROM (
    SELECT '[{"id": 1, "name": "X"}, {"id": 2, "name": "Y"}]' AS json_array_col -- Sample JSON array string
  ) t
) t2
LATERAL VIEW explode(arr) x_tab AS x -- Expanding the array into separate rows
