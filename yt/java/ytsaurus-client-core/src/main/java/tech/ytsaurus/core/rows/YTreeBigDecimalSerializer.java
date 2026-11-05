package tech.ytsaurus.core.rows;

import java.math.BigDecimal;

import tech.ytsaurus.core.common.Decimal;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Serializer for YT decimal values represented by Java {@link BigDecimal}.
 */
@NonNullApi
@NonNullFields
public class YTreeBigDecimalSerializer implements YTreeSerializer<BigDecimal> {
    private final int precision;
    private final int scale;

    public YTreeBigDecimalSerializer(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public void serialize(BigDecimal value, YsonConsumer consumer) {
        byte[] result = Decimal.textToBinary(value.toPlainString(), precision, scale);
        consumer.onString(result, 0, result.length);
    }

    @Override
    public BigDecimal deserialize(YTreeNode node) {
        byte[] data = node.bytesValue();

        String textDecimal = Decimal.binaryToText(data, precision, scale);

        if (textDecimal.equals("nan") || textDecimal.equals("inf") || textDecimal.equals("-inf")) {
            throw new IllegalArgumentException(String.format(
                    "YT Decimal value '%s' is not supported by Java BigDecimal", textDecimal));
        }

        return new BigDecimal(textDecimal);
    }

    @Override
    public TiType getColumnValueType() {
        return TiType.decimal(precision, scale);
    }
}
