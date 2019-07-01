package ru.yandex.yt.ytclient.object;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SmallObjectWithComplexFieldAndStateSupportTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(LargeUnflattenObjectClassWithStateSupportTest.class);

    @Test
    public void testSerializationDeserialization() {
        final List<SmallObjectWithComplexFieldAndStateSupport> list = new ArrayList<>();
        final ObjectsMetadata<SmallObjectWithComplexFieldAndStateSupport> meta =
                ObjectsMetadata.getMetadata(SmallObjectWithComplexFieldAndStateSupport.class, list::add);


        final SmallObjectWithComplexFieldAndStateSupport object = meta.generateObjects(1).get(0);
        object.saveYTreeObjectState();

        final SmallObjectWithComplexFieldAndStateSupport prevState = object.getYTreeObjectState();
        Assert.assertNotSame(object.getComplexField(), prevState.getComplexField());
        Assert.assertNotSame(object.getComplexField().iterator().next(), prevState.getComplexField().iterator().next());
        Assert.assertNotSame(object.getComplexField().iterator().next().values().iterator().next(),
                prevState.getComplexField().iterator().next().values().iterator().next());
        Assert.assertNotSame(object.getComplexField().iterator().next().values().iterator().next().iterator().next(),
                prevState.getComplexField().iterator().next().values().iterator().next().iterator().next());

        object.getComplexField().iterator().next().values().iterator().next().iterator().next().setIntField(9991001);

        final SmallObjectWithComplexFieldAndStateSupport expect = new SmallObjectWithComplexFieldAndStateSupport();
        expect.setKeyField(object.getKeyField());
        expect.setComplexField(object.getComplexField());

        list.clear();
        meta.deserializeMappedObjects(meta.serializeMappedObjects(Collections.singletonList(object)));

        LOGGER.info("Before: {}", object);
        LOGGER.info(" After: {}", list.get(0));

        // complexField изменился - и он будет сериализован
        Assert.assertEquals(expect, list.get(0));

    }
}
