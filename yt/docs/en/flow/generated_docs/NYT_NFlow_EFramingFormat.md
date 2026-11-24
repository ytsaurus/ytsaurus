## NYT::NFlow::EFramingFormat {#NYT_NFlow_EFramingFormat}
<!-- This page is generated. -->

#|
|| Possible values | **Description** ||
|| `auto` | Detects `protoseq` or `lenval` while unpacking; it cannot detect `light_protoseq`. Equivalent to `protoseq` while packing. ||
|| `protoseq` | A four-byte length and a synchronization marker after every frame; the markers allow reading to resume after a partially corrupted frame. ||
|| `lenval` | A varint length before every frame; a compact format without recovery after partial corruption. ||
|| `light_protoseq` | A four-byte length before every frame without a synchronization marker; it is not auto-detected and cannot recover after partial corruption. ||
|#

