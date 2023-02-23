package tech.ytsaurus.typeinfo;

public abstract class ExtensionType extends TiType {
    final String system;

    protected ExtensionType(String system) {
        super(TypeName.Extension);
        this.system = system;
    }
}
