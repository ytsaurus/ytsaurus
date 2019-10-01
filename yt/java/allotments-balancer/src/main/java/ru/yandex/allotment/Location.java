package ru.yandex.allotment;

import java.util.Map;

import ru.yandex.inside.yt.kosher.ytree.YTreeNode;

public class Location {
    public final String id;
    public final int allotmentIndex;
    public final int primaryAllotmentIndex;

    public Location(Map<String, YTreeNode> attributes) {
        this.id = attributes.get("location_id").stringValue();
        this.allotmentIndex = attributes.get("allotment_index").intValue();
        this.primaryAllotmentIndex = attributes.get("primary_allotment_index").intValue();
    }
}
