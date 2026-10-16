# Ip

В модуле `Ip` поддерживаются как IPv4, так и IPv6 адреса. По умолчанию они представляются в виде бинарных строк длиной 4 и 16 байт соответственно.

## Список функций

* `Ip::FromString(String{Flags:AutoMap}) -> String?` &mdash; из человекочитаемого представления в бинарное
* `Ip::SubnetFromString(String{Flags:AutoMap}) -> String?` &mdash; из человекочитаемого представления подсети в бинарное
* `Ip::ToString(String{Flags:AutoMap}) -> String?` &mdash; из бинарного представления в человекочитаемое
* `Ip::SubnetToString(String{Flags:AutoMap}) -> String?` &mdash; из бинарного представления подсети в человекочитаемое
* `Ip::Ipv4FromUint32(Uint32{Flags:AutoMap}) -> String` &mdash; из Uint32 в бинарное представление; целое число `A << 24 | B << 16 | C << 8 | D` соответствует адресу `A.B.C.D`
* `Ip::Ipv4ToUint32(String{Flags:AutoMap}) -> Uint32?` &mdash; из бинарного представления IPv4 в Uint32; адрес `A.B.C.D` соответствует целому числу `A << 24 | B << 16 | C << 8 | D`; IPv6 не поддерживается
* `Ip::IsIPv4(String?) -> Bool`
* `Ip::IsIPv6(String?) -> Bool`
* `Ip::IsEmbeddedIPv4(String?) -> Bool`
* `Ip::ConvertToIPv6(String{Flags:AutoMap}) -> String` &mdash; IPv6 остается без изменений, а IPv4 становится embedded в IPv6
* `Ip::GetSubnet(String{Flags:AutoMap}, [Uint8?]) -> String` &mdash; во втором аргументе размер подсети, по умолчанию 24 для IPv4 и 64 для IPv6
* `Ip::GetSubnetByMask(String{Flags:AutoMap}, String{Flags:AutoMap}) -> String` &mdash; во втором аргументе битовая маска подсети
* `Ip::SubnetMatch(String{Flags:AutoMap}, String{Flags:AutoMap}) -> Bool` &mdash; в первом аргументе подсеть, во втором аргументе подсеть или адрес

## Примеры

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

SELECT Ip::ToString(
  Ip::Ipv4FromUint32(1)
); -- "0.0.0.1"

SELECT Ip::Ipv4ToUint32(
  Ip::FromString("0.0.0.1")
); -- 1
```
