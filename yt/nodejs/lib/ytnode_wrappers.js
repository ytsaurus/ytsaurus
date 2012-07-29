var binding = require("./ytnode");

for (var p in binding) {
    if (/^(ECompression|EDataType)/.test(p)) {
        exports[p] = binding[p];
    }
}
