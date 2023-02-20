package tech.ytsaurus.client;

import java.net.InetAddress;
import java.net.UnknownHostException;

import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * Enum representing common Yandex datacenters
 */
@NonNullApi
@NonNullFields
public enum DC {

    SAS("sas"),
    VLA("vla"),
    IVA("iva"),
    MAN("man"),
    MYT("myt"),
    // this value should not be used in client code
    UNKNOWN("unknown");

    private final String prefix;

    DC(String prefix) {
        this.prefix = prefix;
    }

    /**
     * Attempts to initialize datacenter corresponding to a given hostname
     *
     * @return data center enum value or UNKNOWN if resolution fails
     */
    public static DC fromHostName(String hostName) {
        String prefix = hostName.substring(0, Math.min(3, hostName.length()));
        switch (prefix) {
            case "sas":
                return SAS;
            case "vla":
                return VLA;
            case "iva":
                return IVA;
            case "myt":
                return MYT;
            case "man":
                return MAN;
            default:
                return UNKNOWN;

        }
    }

    /**
     * Attempts to get current DC name
     *
     * @return current DC enum value or UNKNOWN if resolution fails
     */
    public static DC getCurrentDc() {
        try {
            return fromHostName(InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            return DC.UNKNOWN;
        }
    }

    public String prefix() {
        return prefix;
    }
}
