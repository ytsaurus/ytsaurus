package ru.yandex.inside.yt.kosher.ytree;

/**
 * @author sankear
 */
public interface YTreeStringNode extends YTreeScalarNode<String> {

    String getValue();

    String setValue(String value);

    byte[] getBytes();

    byte[] setBytes(byte[] bytes);

}
