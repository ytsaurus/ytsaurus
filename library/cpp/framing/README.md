The library provides methods to serialize sequence of frames (array of bytes) into output stream as sequence of bytes
and to extract original frames from sequence of bytes.

The library is used to concatenate serialized protobufs and sent to `logbroker`,
and supports 2 codecs (`protoseq` and `lenval`).
`logfeller` has already supported extracting original protobufs from such messages (see chunk_splitter `protoseq` and `lenval`).
`push-client` supports only `protoseq`, but has its own implementation.
`unified-agent` supports both formats.

For more information about `protoseq` format see [wiki](https://wiki.yandex-team.ru/logfeller/splitter/protoseq/).

`protoseq` format is more expensive, but supports restoring of partially corrupted messages.
`lenval` is lightweigth and does not support restoring of partially corrupted messages.

`lenval` codec is preferable format when transport and storage is reliable, because it has no overhead.

When format of message is unknown, it is recommended to use format `auto` to guess original format automatically. 

### Examples:

#### Packer

```
#include <library/cpp/framing/packer.h>

TBufferOutput out;
NFraming::TPacker packer(EFormat::Lenval);
TMessage someMessage;
packer.Add(someMessage);
TMessage anotherMessage;
packer.Add(anotherMessage);
packer.Flush();
```

#### Unpacker
```
#include <library/cpp/framing/unpacker.h>

TString lbMessage = GetMessageFromLogBroker();
NFraming::TUnpacker unpacker(EFormat::Auto, lbMessage);

TMessage protoMessage;
TStringBuf skip; // will contains broken part of original message 
while (unpacker.NextFrame(protoMessage, skip)) {
    processMessage(protoMessage);
}
```
