package tech.ytsaurus.skiff.schema;

public enum WireType {
    NOTHING,  /* "nothing" */
    INT_8,     /* "int8" */
    INT_16,    /* "int16" */
    INT_32,    /* "int32" */
    INT_64,    /* "int64" */
    INT_128,   /* "int128" */
    UINT_8,    /* "uint8" */
    UINT_16,   /* "uint16" */
    UINT_32,   /* "uint32" */
    UINT_64,   /* "uint64" */
    UINT_128,  /* "uint128" */
    DOUBLE,   /* "double" */
    BOOLEAN,  /* "boolean" */
    STRING_32, /* "string32" */
    YSON_32,   /* "yson32" */

    TUPLE,             /* "tuple" */
    VARIANT_8,          /* "variant8" */
    VARIANT_16,         /* "variant16" */
    REPEATED_VARIANT_8,  /* "repeated_variant8" */
    REPEATED_VARIANT_16; /* "repeated_variant16" */

    @Override
    public String toString() {
        switch (this) {
            case INT_8:
                return "int8";
            case INT_16:
                return "int16";
            case INT_32:
                return "int32";
            case INT_64:
                return "int64";
            case INT_128:
                return "int128";

            case UINT_8:
                return "uint8";
            case UINT_16:
                return "uint16";
            case UINT_32:
                return "uint32";
            case UINT_64:
                return "uint64";
            case UINT_128:
                return "uint128";

            case DOUBLE:
                return "double";
            case BOOLEAN:
                return "boolean";
            case STRING_32:
                return "string32";
            case YSON_32:
                return "yson32";
            case NOTHING:
                return "nothing";

            case TUPLE:
                return "tuple";
            case VARIANT_8:
                return "variant8";
            case VARIANT_16:
                return "variant16";
            case REPEATED_VARIANT_8:
                return "repeated_variant8";
            case REPEATED_VARIANT_16:
                return "repeated_variant16";

            default:
                throw new IllegalStateException("Illegal enum state");
        }
    }

    public boolean isSimpleType() {
        switch (this) {
            case INT_8:
            case INT_16:
            case INT_32:
            case INT_64:
            case INT_128:

            case UINT_8:
            case UINT_16:
            case UINT_32:
            case UINT_64:
            case UINT_128:

            case DOUBLE:
            case BOOLEAN:
            case STRING_32:
            case YSON_32:
            case NOTHING:
                return true;

            default:
                return false;
        }
    }

    public boolean isVariant() {
        switch (this) {
            case VARIANT_8:
            case VARIANT_16:
                return true;

            default:
                return false;
        }
    }
}
