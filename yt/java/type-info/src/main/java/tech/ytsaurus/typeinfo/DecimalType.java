package tech.ytsaurus.typeinfo;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.yson.YsonConsumer;

public class DecimalType extends TiType {
    public static final int MAX_PRECISION = 35;

    final int precision;
    final int scale;

    public DecimalType(int precision, int scale) {
        super(TypeName.Decimal);
        if (precision <= 0 || precision > MAX_PRECISION) {
            throw new RuntimeException(String.format("TODO: precision must be in range [0, %d]", MAX_PRECISION));
        }

        if (scale < 0 || scale > MAX_PRECISION) {
            throw new RuntimeException(String.format("TODO: scale must be in range [0, precision (=%d)]", precision));
        }

        this.precision = precision;
        this.scale = scale;
    }

    /**
     * Get precision.
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * Get scale.
     */
    public int getScale() {
        return scale;
    }

    @Override
    public String toString() {
        return String.format("Decimal(%d, %d)", precision, scale);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DecimalType that = (DecimalType) o;
        return this.precision == that.precision &&
                this.scale == that.scale;
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeName, precision, scale);
    }

    @Override
    public void serializeTo(YsonConsumer ysonConsumer) {
        ysonConsumer.onBeginMap();

        assert TypeName.Decimal.wireNameBytes != null;
        YsonConsumer.onKeyedItem(ysonConsumer, KeyNames.TYPE_NAME);
        YsonConsumer.onString(ysonConsumer, TypeName.Decimal.wireNameBytes);

        YsonConsumer.onKeyedItem(ysonConsumer, KeyNames.PRECISION);
        ysonConsumer.onInteger(precision);

        YsonConsumer.onKeyedItem(ysonConsumer, KeyNames.SCALE);
        ysonConsumer.onInteger(scale);

        ysonConsumer.onEndMap();
    }
}
