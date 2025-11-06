-- Example of extracting data from a JSON string using the from_json function
SELECT
  data.name, -- Selects the name field from the JSON
  data.age, -- Selects the age field from the JSON
  data.contacts.email AS email -- Selects the email field from the contacts struct within the JSON
FROM (
  SELECT from_json(
    '{"name":"Alice","age":30,"contacts":{"email":"alice@example.com"}}', -- JSON string to be parsed
    'name STRING, age INT, contacts STRUCT<email:STRING>' -- Schema definition for the JSON fields
  ) AS data
) t
