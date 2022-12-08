package tech.ytsaurus.type_info;

public abstract class ExtensionType extends TiType {
    final String system;

    protected ExtensionType(String system) {
        super(TypeName.Extension);
        this.system = system;
    }
}
