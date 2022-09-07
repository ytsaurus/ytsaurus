package ru.yandex.inside.yt.kosher.ytree;

/**
 * @author sankear
 */
public interface YTreeBooleanNode extends YTreeScalarNode<Boolean>  {

    boolean getValue();

    boolean setValue(boolean value);

}
