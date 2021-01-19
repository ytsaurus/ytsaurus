package ru.yandex.yt.canonize_schema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeListNode;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;

class CanonizeSchemaTest {
    @Test
    public void testCanonizeSchema() {
        String noncanonizedYson =
            "<" +
                "\"strict\" = %true;" +
                "\"unique_keys\" = %false;" +
            "> [" +
                "{" +
                    "\"type_v3\" = {" +
                        "\"type_name\" = \"optional\";" +
                        "\"item\" = \"string\";" +
                    "};" +
                    "\"type\" = \"string\";" +
                    "\"required\" = %false;" +
                    "\"type_v2\" = {" +
                        "\"metatype\" = \"optional\";" +
                        "\"element\" = \"string\";" +
                    "};" +
                    "\"name\" = \"Name\";" +
                "};" +
                "{" +
                    "\"type_v3\" = {" +
                        "\"type_name\" = \"list\";" +
                        "\"item\" = {" +
                            "\"type_name\" = \"struct\";" +
                            "\"members\" = [" +
                                "{" +
                                    "\"type\" = \"int64\";" +
                                    "\"name\" = \"X\";" +
                                "};" +
                                "{" +
                                    "\"type\" = \"int64\";" +
                                    "\"name\" = \"Y\";" +
                                "};" +
                            "];" +
                        "};" +
                    "};" +
                    "\"type\" = \"any\";" +
                    "\"required\" = %true;" +
                    "\"type_v2\" = {" +
                        "\"metatype\" = \"list\";" +
                        "\"element\" = {" +
                            "\"metatype\" = \"struct\";" +
                            "\"fields\" = [" +
                                "{" +
                                    "\"name\" = \"X\";" +
                                    "\"type\" = \"int64\";" +
                                "};" +
                                "{" +
                                    "\"name\" = \"Y\";" +
                                    "\"type\" = \"int64\";" +
                                "};" +
                            "];" +
                        "};" +
                    "};" +
                    "\"name\" = \"Points\";" +
                "};" +
            "]";

        YTreeNode noncanonized = YTreeTextSerializer.deserialize(noncanonizedYson);
        YTreeListNode canonized = CanonizeSchema.canonizeSchema(noncanonized.listNode());

        String canonizedText = YTreeTextSerializer.stableSerialize(canonized);
        Assertions.assertEquals(
                "<\"strict\"=%true;\"unique_keys\"=%false>[{\"name\"=\"Name\";" +
                        "\"type_v3\"={\"item\"=\"string\";\"type_name\"=\"optional\"}};{\"name\"=\"Points\";" +
                        "\"type_v3\"={\"item\"={\"members\"=[{\"name\"=\"X\";\"type\"=\"int64\"};{\"name\"=\"Y\";" +
                        "\"type\"=\"int64\"}];\"type_name\"=\"struct\"};\"type_name\"=\"list\"}}]",
                canonizedText);
    }
}
