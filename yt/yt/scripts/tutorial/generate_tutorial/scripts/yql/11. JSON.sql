-- Example of working with a comlicated JSON object
SELECT * FROM (
  SELECT * FROM (
     -- Parsing the JSON into a list of structs
    SELECT Yson::ConvertTo(json_array_col, List<Struct<id: Int32, name:Text, value:Double>>) as arr
    FROM (
      SELECT '[{"id": 1, "name": "Foo", "value":-2.5}, {"id": 2, "name": "Bar", "value": 3.14}]'j AS json_array_col -- Sample JSON literal string
    )
  ) FLATTEN LIST BY arr -- Expanding the list into separate rows of structures
) FLATTEN COLUMNS -- Expanding the structure into separate columns
