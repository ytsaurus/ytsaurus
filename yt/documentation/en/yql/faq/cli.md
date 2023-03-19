---
vcsPath: yql/docs_yfm/docs/ru/yql-product/faq/cli.md
sourcePath: yql-product/faq/cli.md
---
# F.A.Q. about the console client

## How do I revert to a human-readable UTF-8 a text in CSV/TSV modes of the console client?
In this mode, all the column values are C-escaped so that a table row can be unambiguously split into columns by tabulation characters. You can convert your text back to readable format using `UnescapeC` from the C++ flavor in Arcadia or in Python as follows:

```python
print(b'\xf0\x9f\x98\x81\xf0\x9f\x8e\xaf\xf0\x9f\x92\xaf'.decode('utf-8'))
😁🎯💯

print(bytes(b'\\xf0\\x9f\\x98\\x81\\xf0\\x9f\\x8e\\xaf\\xf0\\x9f\\x92\\xaf'.decode('unicode_escape'), 'latin-1').decode('utf-8'))
😁🎯💯
```

## Why does the operation fail to run when I press Enter in the interactive mode?
In the interactive mode, the system can't tell reliably which response does the user expect when they press Enter: run the operation or start a new line. If you select only one behavior of the two, then for some queries it would be convenient and for other queries not. That's why the console client tries to guess what you expected.
