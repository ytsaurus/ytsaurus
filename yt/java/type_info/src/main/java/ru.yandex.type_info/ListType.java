package ru.yandex.type_info;

public class ListType extends ItemizedType {
    ListType(TiType element) {
        super(TypeName.List, element);
    }

    @Override
    public String toString() {
        return String.format("List<%s>", item);
    }
}
