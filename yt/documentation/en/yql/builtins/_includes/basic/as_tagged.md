
## AsTagged, Untag {#as-tagged}

Wraps a value in [Tagged data type](../../../types/special.md) with the specified tag preserving the physical data type. `Untag`: Reverse operation.

**Signature**
```
AsTagged(T, tagName:String)->Tagged<T,tagName>
AsTagged(T?, tagName:String)->Tagged<T,tagName>?

Untag(Tagged<T, tagName>)->T
Untag(Tagged<T, tagName>?)->T?
```

Mandatory arguments:

1. Value of an arbitrary type.
2. Tag name.

Returns a copy of the value from the first argument with the specified tag in the data type.

Examples of use cases:

* Returning to the client to display media files of base64-encoded strings in the web interface. <!--Support for tags in the YQL web UI is [described here](../../../interfaces/web_tagged.md).-->
* Protecting at the boundaries of the UDF call against the transfer of incorrect values.
* Additional clarifications at the returned column type level.
