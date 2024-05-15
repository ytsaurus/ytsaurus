package tech.ytsaurus.client.discovery;

import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.TMemberInfo;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ytree.TAttribute;
import tech.ytsaurus.ytree.TAttributeDictionary;

public class MemberInfo {
    private final String id;
    private final long priority;
    private final long revision;
    @Nullable
    private final Map<String, YTreeNode> attributes;

    public MemberInfo(String id, long priority, long revision, Map<String, YTreeNode> attributes) {
        this.id = id;
        this.priority = priority;
        this.revision = revision;
        this.attributes = attributes;
    }

    public MemberInfo(String id, long priority, long revision) {
        this(id, priority, revision, null);
    }

    public static MemberInfo fromProto(TMemberInfo protoValue) {
        Map<String, YTreeNode> attributes = null;
        if (protoValue.hasAttributes()) {
            attributes = protoValue.getAttributes().getAttributesList().stream()
                    .collect(Collectors.toMap(
                            TAttribute::getKey,
                            attr -> YTreeBinarySerializer.deserialize(attr.getValue().newInput())));
        }
        return new MemberInfo(protoValue.getId(), protoValue.getPriority(), protoValue.getRevision(), attributes);
    }

    public TMemberInfo toProto() {
        TMemberInfo.Builder builder = TMemberInfo.newBuilder();
        builder.setId(id);
        builder.setPriority(priority);
        builder.setRevision(revision);
        if (attributes != null) {
            final TAttributeDictionary.Builder aBuilder = builder.getAttributesBuilder();
            for (Map.Entry<String, YTreeNode> me : attributes.entrySet()) {
                aBuilder.addAttributesBuilder()
                        .setKey(me.getKey())
                        .setValue(ByteString.copyFrom(me.getValue().toBinary()));
            }
        }
        return builder.build();
    }

    public String getId() {
        return id;
    }

    public long getPriority() {
        return priority;
    }

    public long getRevision() {
        return revision;
    }

    @Nullable
    public Map<String, YTreeNode> getAttributes() {
        return attributes;
    }
}
