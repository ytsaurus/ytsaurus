# Xml UDF

Функции для работы с XML.
``` yql
Xml::Parse(String?) -> Resource<Xml.Document>?
Xml::Root(Resource<Xml.Document>?) -> Resource<Xml.Node>?
Xml::XPath(Resource<Xml.Node>?, String) -> List<Resource<Xml.Node>>
Xml::ConvertToBool(Resource<Xml.Node>?) -> Bool?
Xml::ConvertToInt64(Resource<Xml.Node>?) -> Int64?
Xml::ConvertToUint64(Resource<Xml.Node>?) -> Uint64?
Xml::ConvertToDouble(Resource<Xml.Node>?) -> Double?
Xml::ConvertToString(Resource<Xml.Node>?) -> String?
Xml::XPathGetBool(Resource<Xml.Node>?, String) -> Bool?
Xml::XPathGetInt64(Resource<Xml.Node>?, String) -> Int64?
Xml::XPathGetUint64(Resource<Xml.Node>?, String) -> Uint64?
Xml::XPathGetDouble(Resource<Xml.Node>?, String) -> Double?
Xml::XPathGetString(Resource<Xml.Node>?, String) -> String?
Xml::Serialize(Resource<Xml.Node?>) -> String?
```
Функции вида `Xml::XPathGetFoo` — shortcut для `Xml::XPath` с путём из второго аргумента и вызова `Xml::ConvertToFoo` на первом элементе из результирующего списка.

#### Примеры

``` yql
$document = Xml::Parse("<root><a>-1</a><a>0</a><a>1</a></root>");
$root = Xml::Root($document);
SELECT ListMap(Xml::XPath($root, "//a"), Xml::ConvertToInt64); -- [-1, 0, 1]
```

``` yql
$document = Xml::Parse("<root><a>-1</a><a>0</a><a>1</a></root>");
$root = Xml::Root($document);
SELECT Xml::Serialize($root); -- "<root><a>-1</a><a>0</a><a>1</a></root>"
```
