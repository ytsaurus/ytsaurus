package ru.yandex.allotment;

import java.util.Map;

import ru.yandex.inside.yt.kosher.ytree.YTreeNode;

public class Location {
    public final String id;
    public final int allotmentIndex;
    public final int primaryAllotmentIndex;
    public final boolean isDead;

    public Location(Map<String, YTreeNode> attributes) {
        this.id = attributes.get("location_id").stringValue();
        this.allotmentIndex = attributes.get("allotment_index").intValue();
        this.primaryAllotmentIndex = attributes.get("primary_allotment_index").intValue();
        this.isDead = false;
    }

    public Location(String id, int allotmentIndex, int primaryAllotmentIndex) {
        this.id = id;
        this.allotmentIndex = allotmentIndex;
        this.primaryAllotmentIndex = primaryAllotmentIndex;
        this.isDead = true;
    }
}
