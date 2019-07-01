package ru.yandex.yt.ytclient.object;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LargeUnflattenObjectClassWithStateSupportTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(LargeUnflattenObjectClassWithStateSupportTest.class);

    @Test
    public void testSerializationDeserialization() {
        final List<LargeUnflattenObjectClassWithStateSupport> list = new ArrayList<>();
        final ObjectsMetadata<LargeUnflattenObjectClassWithStateSupport> meta =
                ObjectsMetadata.getMetadata(LargeUnflattenObjectClassWithStateSupport.class, list::add);


        final LargeUnflattenObjectClassWithStateSupport object = meta.generateObjects(1).get(0);
        object.saveYTreeObjectState();
        object.getObject1().setLongField2(object.getObject1().getLongField2() + 1);

        final LargeUnflattenObjectClassWithStateSupport expect = new LargeUnflattenObjectClassWithStateSupport();
        expect.setIntField1(object.getIntField1());
        expect.setObject1(object.getObject1());


        list.clear();
        meta.deserializeMappedObjects(meta.serializeMappedObjects(Collections.singletonList(object)));

        LOGGER.info("Before: {}", object);
        LOGGER.info(" After: {}", list.get(0));

        // object1 изменился - и он будет сериализован
        Assert.assertEquals(expect, list.get(0));

    }
}
