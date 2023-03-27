package tech.ytsaurus.client.rows;

import java.util.ArrayList;
import java.util.List;

import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.skiff.ComplexSchema;
import tech.ytsaurus.skiff.SkiffSchema;
import tech.ytsaurus.skiff.WireType;
import tech.ytsaurus.typeinfo.TiType;

import static tech.ytsaurus.client.rows.WireTypeUtil.getWireTypeOf;

public class SchemaConverter {

    private SchemaConverter() {
    }

    public static SkiffSchema toSkiffSchema(TableSchema tableSchema) {
        ComplexSchema schema = SkiffSchema.tuple(new ArrayList<>());
        for (var column : tableSchema.getColumns()) {
            schema.getChildren().add(
                    getTiTypeSchema(column.getTypeV3())
                            .setName(column.getName())
            );
        }
        return schema;
    }

    private static SkiffSchema getTiTypeSchema(TiType tiType) {
        WireType wireType = getWireTypeOf(tiType);
        if (wireType.isSimpleType()) {
            return SkiffSchema.simpleType(wireType);
        }
        return getComplexTiTypeSchema(tiType);
    }

    private static ComplexSchema getComplexTiTypeSchema(TiType tiType) {
        if (tiType.isOptional()) {
            return SkiffSchema.variant8(
                    List.of(SkiffSchema.nothing(),
                            getTiTypeSchema(tiType.asOptional().getItem())
                    )
            );
        }
        if (tiType.isStruct()) {
            ComplexSchema schema = SkiffSchema.tuple(new ArrayList<>());
            for (var member : tiType.asStruct().getMembers()) {
                schema.getChildren().add(
                        getTiTypeSchema(member.getType())
                                .setName(member.getName())
                );
            }
            return schema;
        }
        if (tiType.isList()) {
            return SkiffSchema.repeatedVariant8(
                    List.of(getTiTypeSchema(tiType.asList().getItem()))
            );
        }
        if (tiType.isDict()) {
            return SkiffSchema.repeatedVariant8(
                    List.of(SkiffSchema.tuple(
                            List.of(getTiTypeSchema(tiType.asDict().getKey()),
                                    getTiTypeSchema(tiType.asDict().getValue())))
                    )
            );
        }
        throw new IllegalStateException("Unsupported TiType: " + tiType);
    }
}
