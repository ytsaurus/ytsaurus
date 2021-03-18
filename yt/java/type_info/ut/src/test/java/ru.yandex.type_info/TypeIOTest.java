package ru.yandex.type_info;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TypeIOTest {
    @Test
    public void testParseNonParametrizedTypes() {
        Assertions.assertEquals(TypeIO.parseYson("int64"), TiType.int64());
        Assertions.assertEquals(TypeIO.parseYson("bool"), TiType.bool());
        Assertions.assertEquals(TypeIO.parseYson("{type_name=int64}"), TiType.int64());
    }

    @Test
    public void testParseOptionalType() {
        Assertions.assertEquals(TypeIO.parseYson("{type_name=optional; item=int64}"), TiType.optional(TiType.int64()));
        Assertions.assertEquals(
                TypeIO.parseYson("{type_name=optional; item={type_name=optional; item=int64}}"),
                TiType.optional(TiType.optional(TiType.int64())));
        Assertions.assertEquals(
                TypeIO.parseYson("{type_name=optional; item={type_name=optional; item={type_name=int64}}}"),
                TiType.optional(TiType.optional(TiType.int64())));
    }

    @Test
    public void testParseListType() {
        Assertions.assertEquals(
                TypeIO.parseYson("{type_name=list; item=int64}"),
                TiType.list(TiType.int64()));
        Assertions.assertEquals(
                TypeIO.parseYson("{type_name=list; item={type_name=list; item=int64}}"),
                TiType.list(TiType.list(TiType.int64())));
        Assertions.assertEquals(
                TypeIO.parseYson("{type_name=optional; item={type_name=optional; item={type_name=int64}}}"),
                TiType.optional(TiType.optional(TiType.int64())));
    }

    @Test
    public void testParseTaggedType() {
        Assertions.assertEquals(
                TypeIO.parseYson("{type_name=list; item=int64}"),
                TiType.list(TiType.int64()));
    }

    @Test
    public void testParseDecimalType() {
        Assertions.assertEquals(
                TypeIO.parseYson("{type_name=decimal; precision=3; scale=2}"),
                TiType.decimal(3, 2)
        );
    }

    @Test
    public void testParseStructType() {
        Assertions.assertEquals(
                TypeIO.parseYson("{type_name=struct; members=[{name=foo; type=int64};{name=bar; type=string}]}"),
                TiType.structBuilder()
                        .add("foo", TiType.int64())
                        .add("bar", TiType.string())
                        .build());
    }

    @Test
    public void testParseTupleType() {
        Assertions.assertEquals(
                TypeIO.parseYson("{type_name=tuple; elements=[{type=int64};{type=string}]}"),
                TiType.tuple(TiType.int64(), TiType.string()));
    }

    @Test
    public void testParseVariant() {
        Assertions.assertEquals(
                TypeIO.parseYson("{type_name=variant; elements=[{type=int64};{type=string}]}"),
                TiType.variantOverTuple(TiType.int64(), TiType.string()));

        Assertions.assertEquals(
                TypeIO.parseYson("{type_name=variant; members=[{name=a; type=int64};{name=b; type=string}]}"),
                TiType.variantOverStructBuilder()
                    .add("a", TiType.int64())
                    .add("b", TiType.string())
                    .build());
    }

    @Test
    public void testParseDict() {
        Assertions.assertEquals(
                TypeIO.parseYson("{type_name=dict; key={type_name=optional; item=int64}; value=string}"),
                TiType.dict(TiType.optional(TiType.int64()), TiType.string()));
    }

}
