# Data representation in JSON format

## Bool {#bool}

Boolean value.

* Type in JSON: `bool`.
* Sample {{product-name}} value: `true`.
* Sample JSON value: `true`.

## Int8, Int16, Int32, Int64 {#int}

Signed integer types.

* Type in JSON: `number`.
* Sample {{product-name}} value: `123456`, `-123456`.
* Sample JSON value: `123456`, `-123456`.

## Uint8, Uint16, Uint32, Uint64 {#uint}

Unsigned integer types.

* Type in JSON: `number`.
* Value example {{product-name}} — `123456`.
* JSON value example — `123456`.

## Float {#float}

4-byte real number.

* Type in JSON: `number`.
* Value example {{product-name}} — `0.12345679`.
* JSON value example — `0.12345679`.

## Double {#double}

8-byte real number.

* Type in JSON: `number`.
* Value example {{product-name}} — `0.12345678901234568`.
* JSON value example — `0.12345678901234568`.

## Decimal {#decimal}

Fixed-precision number. Only Decimal(22, 9) is supported.

* Type in JSON: `string`.
* Value example {{product-name}} — `-320.789`.
* JSON value example — `"-320.789"`.

## String, Yson {#string}

Binary strings. Encoding algorithm depending on the byte value:

* [0-31] — `\u00XX` (6 characters that specify the unicode character's code).
* [32-126] — as is. These are readable single-byte characters that don't need to be escaped.
* [127-255] — `\u00XX`.

Decoding is a reverse process. No character codes in excess of 255 are permitted in `\u00XX`.

* Type in JSON: `string`.
* Sample {{product-name}} value: A sequence of 4 bytes:
   * 5 `0x05` — control character.
   * 10 `0x0a` — `\n` line break.
   * 107 `0x6b` — `k` character.
   * 255 `0xff` — `ÿ` unicode character.
* Sample JSON value: `"\u0005\nk\u00FF"`.

## Utf8, Json, Uuid {#utf}

String types in utf-8. Such strings are represented in JSON as strings with JSON characters escaped: `\\`, `\"`, `\n`, `\r`, `\t`, `\f`.

* Type in JSON: `string`.
* Sample {{product-name}} value: C++ code:

   ```c++
   "Escaped characters: "
   "\\ \" \f \b \t \r\n"
   "Non-escaped characters: "
   "/ ' < > & []() ".
   ```

* Sample JSON value: `"Escaped characters: \\ \" \f \b \t \r\nNon-escaped characters: / ' < > & []() "`.

## Date {#date}

Date. Uint64, number of days in unix time.

* Type in JSON: `string`.
* Value example {{product-name}} — `18367`.
* Sample JSON value: `"2020-04-15"`.

## Datetime {#datetime}

Date and time. Uint64, number of seconds in unix time.

* Type in JSON: `string`.
* Value example {{product-name}} — `1586966302`.
* Sample JSON value: `"2020-04-15T15:58:22Z"`.

## Timestamp {#timestamp}

Date and time. Uint64, number of microseconds in unix time.

* Type in JSON: `string`.
* Value example {{product-name}} — `1586966302504185`.
* Sample JSON value: `"2020-04-15T15:58:22.504185Z"`.

## Interval {#interval}

Time interval. Int64, precision down to microseconds. Permitted interval values are up to 24 hours.

* Type in JSON: `number`.
* Sample {{product-name}} value: `123456`, `-123456`.
* Sample JSON value: `123456`, `-123456`.

## Date32 {#date32}

Date. Int32, number of days relative to the unix epoch (can be negative).

* Type in JSON: `string`.
* Sample {{product-name}} value: `-8722`.
* Sample JSON value: `"1946-02-14"`.

## Datetime64 {#datetime64}

Date and time. Int64, number of seconds relative to the unix epoch.

* Type in JSON: `string`.
* Sample {{product-name}} value: `-753511371`.
* Sample JSON value: `"1946-02-14T19:17:09Z"`.

## Timestamp64 {#timestamp64}

Date and time. Int64, number of microseconds relative to the unix epoch.

* Type in JSON: `string`.
* Sample {{product-name}} value: `-753511370765432`.
* Sample JSON value: `"1946-02-14T19:17:09.234568Z"`.

## Interval64 {#interval64}

Time interval. Int64, precision down to microseconds.

* Type in JSON: `number`.
* Sample {{product-name}} value: `123456`, `-123456`.
* Sample JSON value: `123456`, `-123456`.

## TzDate32, TzDatetime64, TzTimestamp64 {#tzdate32}

Date/time types with a timezone label. The point in time is stored in UTC.

* Type in JSON: `string`.
* The value is represented as a string time representation and the timezone, separated by a comma.
* Sample JSON value: `"1946-02-14,Europe/Moscow"`, `"1946-02-14T19:17:09,Europe/Moscow"`, `"1946-02-14T19:17:09.234568,Europe/Moscow"` for TzDate32, TzDatetime64, and TzTimestamp64, respectively.

## Optional {#optional}

Means that the value can be `null`. If the value is `null`, then in JSON it's also `null`. If the value is not `null`, then the JSON value is expressed as if the type isn't `Optional`.

* Type in JSON is missing.
* Sample {{product-name}} value: `null`.
* Sample JSON value: `null`.

## List {#list}

List. An ordered set of values of a given type.

* Type in JSON: `array`.
* Sample {{product-name}} value:
   * type — `List<Int32>`.
   * Value: `1, 10, 100`.
* Sample JSON value: `[1,10,100]`.

## Stream {#stream}

Stream. Single-pass iterator by same-type values,

* Type in JSON: `array`.
* Sample {{product-name}} value:
   * type — `Stream<Int32>`.
   * Value: `1, 10, 100`.
* Sample JSON value: `[1,10,100]`.

## Struct {#struct}

Structure. An unordered set of values with the specified names and type.

* Type in JSON: `object`.
* Sample {{product-name}} value:
   * Type: `Struct<'Id':Uint32,'Name':String,'Value':Int32,'Description':Utf8?>`;
   * Value: `"Id":1,"Name":"Anna","Value":-100,"Description":null`.
* Sample JSON value: `{"Id":1,"Name":"Anna","Value":-100,"Description":null}`.

## Tuple {#tuple}

Tuple. An ordered set of values of the set types.

* Type in JSON: `array`.
* Sample {{product-name}} value:
   * Type: `Tuple<Int32??,Int64???,String??,Utf8???>`;
   * Value: `10,-1,null,"Some string"`.
* Sample JSON value: `[10,-1,null,"Some string"]`.

## Dict {#dict}

Dictionary. An unordered set of key-value pairs. The type is set both for the key and the value. It's written in JSON to an array of arrays including two items.

* Type in JSON: `array`.
* Sample {{product-name}} value:
   * type — `Dict<Int64,String>`;
   * Value: `1:"Value1",2:"Value2"`.
* Sample JSON value: `[[1,"Value1"],[2,"Value2"]]`.

