package ru.yandex.yt.ytclient.object;

import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeKeyField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;

@YTreeObject
public class BrokenIntEnumClass {
    public enum IntEnum implements ru.yandex.misc.enums.IntEnum {
        VI1(1), VI2(2), VI3(3);

        private final int value;

        IntEnum(int value) {
            this.value = value;
        }

        @Override
        public int value() {
            return value;
        }
    }

    @YTreeKeyField
    private int key;
    private IntEnum intEnum;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BrokenIntEnumClass that = (BrokenIntEnumClass) o;
        return key == that.key &&
                intEnum == that.intEnum;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, intEnum);
    }
}
