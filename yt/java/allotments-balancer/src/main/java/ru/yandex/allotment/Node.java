package ru.yandex.allotment;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.bolts.collection.MapF;
import ru.yandex.inside.yt.kosher.Yt;
import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;

public class Node {
    public final String addr;

    public final MapF<String, Location> locations;

    // location -> index
    public final Map<String, Integer> allotmentAssignment;

    private final DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").withZone(ZoneId.of("UTC"));
    ZonedDateTime dateTime;
    public long lastSeenTime;

    public boolean dirty = false;

    private String state = "online";

    public Node(String addr, Map<String, YTreeNode> attributes) {
        this.addr = addr;
        this.allotmentAssignment = Cf.hashMap();

        this.state = attributes.get("state").stringValue();

        Map<String, YTreeNode> allotmentAssignmentRow = attributes.get("allotments_assignment").asMap();

        for (Map.Entry<String, YTreeNode> entity : allotmentAssignmentRow.entrySet()) {
            this.allotmentAssignment.put(entity.getKey(), entity.getValue().intValue());
        }

        this.locations = Cf.hashMap();

        if (isGood()) {
            for (YTreeNode locationRow : attributes.get("statistics").asMap().get("locations").asList()) {
                Map<String, YTreeNode> locationAttributes = locationRow.asMap();
                if (!locationAttributes.containsKey("location_id")) {
                    continue;
                }

                Location location = new Location(locationAttributes);

                this.locations.put(location.id, location);
            }
        }

        String lastSeenTimeRow = attributes.get("last_seen_time").stringValue();

        dateTime = ZonedDateTime.parse(lastSeenTimeRow, format);
        lastSeenTime = dateTime.toEpochSecond();
    }

    public Status getStatus(long now, long warnThreshold) {
        if (!isGood()) {
            if (now - lastSeenTime > warnThreshold) {
                return Status.ERR;
            } else {
                return Status.WARN;
            }
        }

        return Status.OK;
    }

    public boolean isGood() {
        return state.equals("online");
    }

    public String formatLastSeenTime() {
        return format.format(dateTime);
    }

    public Node(String addr, Yt yt) {
        this(addr, yt.cypress().get(YPath.simple("//sys/nodes").child(addr).allAttributes()).asMap());
    }


    public void assignAllotmentToLocation(String locationId, int allotmentIndex) {
        allotmentAssignment.put(locationId, allotmentIndex);
        dirty = true;
    }

    public void upload(Yt yt, GUID tx, boolean dryRun) {
        if (!dirty) {
            return;
        }

        System.out.println(String.format("%s -> %s", addr, allotmentAssignment));

        if (!dryRun) {
            yt.cypress().set(Optional.of(tx), true, YPath.simple("//sys/nodes").child(addr).attribute("allotments_assignment"),
                    YTree.builder().value(allotmentAssignment).build());
        }
    }

    public boolean moveOut(Allotment allotment) {
        boolean flag = false;

        List<String> _remove = Cf.linkedList();

        for (Map.Entry<String, Integer> entry : allotmentAssignment.entrySet()) {
            if (entry.getValue() == allotment.index) {
                flag = true;
                _remove.add(entry.getKey());
                dirty = true;
            }
        }

        for (String name : _remove) {
            allotmentAssignment.remove(name);
        }

        if (flag) {
            System.out.println(String.format("Moving out allotment '%s' from node '%s'", allotment.name, addr));
        }

        return dirty;
    }

    public boolean removeDeadLocations(Allotment allotment) {
        List<String> _remove = Cf.linkedList();

        for (Map.Entry<String, Integer> entry : allotmentAssignment.entrySet()) {
            if (entry.getValue() == allotment.index && !locations.containsKey(entry.getKey())) {
                System.out.println(String.format("Removing dead location '%s' of allotment '%s' from node '%s'", entry.getKey(), allotment.name, addr));
                _remove.add(entry.getKey());
                dirty = true;
            }
        }

        for (String name : _remove) {
            allotmentAssignment.remove(name);
        }

        return dirty;
    }

    int locationsOfAllotment(Allotment allotment) {
        int result = 0;
        for (Map.Entry<String, Integer> entry : allotmentAssignment.entrySet()) {
            if (entry.getValue() == allotment.index) {
                result += 1;
            }
        }
        return result;
    }
}
