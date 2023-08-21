package tech.ytsaurus.ysontree;

public interface YTreeStringNode extends YTreeScalarNode<String> {

    String getValue();

    String setValue(String value);

    byte[] getBytes();

    byte[] setBytes(byte[] bytes);

}
