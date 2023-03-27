package tech.ytsaurus.client.request;

import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.client.TableAttachmentReader;
import tech.ytsaurus.client.rows.EntitySkiffSerializer;
import tech.ytsaurus.client.rows.WireRowSerializer;
import tech.ytsaurus.core.rows.YTreeRowSerializer;
import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.rpcproxy.ERowsetFormat;

import static tech.ytsaurus.client.rows.EntityUtil.isEntityAnnotationPresent;

public class SerializationContext<T> {
    @Nullable
    protected WireRowSerializer<T> wireSerializer;
    @Nullable
    protected YTreeSerializer<T> ytreeSerializer;
    @Nullable
    private EntitySkiffSerializer<T> skiffSerializer;
    @Nullable
    private Class<T> objectClass;
    protected ERowsetFormat rowsetFormat = ERowsetFormat.RF_YT_WIRE;
    @Nullable
    protected Format format = null;
    @Nullable
    protected TableAttachmentReader<T> attachmentReader = null;

    protected SerializationContext() {
    }

    public SerializationContext(YTreeSerializer<T> serializer, Format format) {
        if (!(serializer instanceof YTreeRowSerializer)) {
            throw new IllegalArgumentException("YTreeRowSerializer was expected");
        }
        this.ytreeSerializer = serializer;
        this.rowsetFormat = ERowsetFormat.RF_FORMAT;
        this.format = format;
    }

    public SerializationContext(Class<T> objectClazz) {
        this.objectClass = objectClazz;
        if (!isEntityAnnotationPresent(objectClazz)) {
            return;
        }
        this.skiffSerializer = new EntitySkiffSerializer<>(objectClazz);
        this.rowsetFormat = ERowsetFormat.RF_FORMAT;
        this.attachmentReader = TableAttachmentReader.skiff(skiffSerializer);
    }

    public SerializationContext(YTreeSerializer<T> serializer) {
        this.ytreeSerializer = serializer;
    }

    public Optional<WireRowSerializer<T>> getWireSerializer() {
        return Optional.ofNullable(this.wireSerializer);
    }

    public Optional<YTreeSerializer<T>> getYtreeSerializer() {
        return Optional.ofNullable(this.ytreeSerializer);
    }

    public Optional<EntitySkiffSerializer<T>> getSkiffSerializer() {
        return Optional.ofNullable(this.skiffSerializer);
    }

    public Optional<Class<T>> getObjectClass() {
        return Optional.ofNullable(this.objectClass);
    }

    public ERowsetFormat getRowsetFormat() {
        return rowsetFormat;
    }

    public Optional<Format> getFormat() {
        if (skiffSerializer != null) {
            return skiffSerializer.getFormat();
        }
        return Optional.ofNullable(format);
    }

    public Optional<TableAttachmentReader<T>> getAttachmentReader() {
        return Optional.ofNullable(attachmentReader);
    }
}
