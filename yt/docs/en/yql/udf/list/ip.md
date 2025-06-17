# Ip

The `Ip` module supports both the IPv4 and IPv6 addresses. By default, they are represented as binary strings of 4 and 16 bytes, respectively.

## List of functions

* `Ip::FromString(String{Flags:AutoMap}) -> String?` — From a human-readable representation to a binary representation.
* `Ip::SubnetFromString(String{Flags:AutoMap}) -> String?` — From a human-readable representation of a subnet to a binary representation.
* `Ip::ToString(String{Flags:AutoMap}) -> String?` — From a binary representation to a human-readable representation.
* `Ip::ToString(String{Flags:AutoMap}) -> String?` — From a binary representation of a subnet to a human-readable representation.
* `Ip::IsIPv4(String?) -> Bool`
* `Ip::IsIPv6(String?) -> Bool`
* `Ip::IsEmbeddedIPv4(String?) -> Bool`
* `Ip::ConvertToIPv6(String{Flags:AutoMap}) -> String` — IPv6 remains unchanged, and IPv4 becomes embedded in IPv6.
* `Ip::GetSubnet(String{Flags:AutoMap}, [Uint8?]) -> String` — The second argument is the subnet size; by default, it's 24 for IPv4 and 64 for IPv6.
* `Ip::GetSubnetByMask(String{Flags:AutoMap}, String{Flags:AutoMap}) -> String` — The second argument is the subnet bitmask.
* `Ip::SubnetMatch(String{Flags:AutoMap}, String{Flags:AutoMap}) -> Bool` — The first argument is the subnet, the second argument is the subnet or address.

## Examples

```yql
SELECT Ip::IsEmbeddedIPv4(
  Ip::FromString("::ffff:77.75.155.3")
); -- true

SELECT
  Ip::ToString(
    Ip::GetSubnet(
      Ip::FromString("213.180.193.3")
    )
  ); -- "213.180.193.0"

SELECT
  Ip::SubnetMatch(
    Ip::SubnetFromString("192.168.0.1/16"),
    Ip::FromString("192.168.1.14"),
  ); -- true

SELECT
  Ip::ToString(
    Ip::GetSubnetByMask(
      Ip::FromString("192.168.0.1"),
      Ip::FromString("255.255.0.0")
    )
  ); -- "192.168.0.0"
```
