Default tcmalloc allocator (8k  pages)
=============

```

Payload size: 4096      extra: 0        would allocate: 4096    overhead: 0%
Payload size: 8192      extra: 0        would allocate: 8192    overhead: 0%
Payload size: 16384     extra: 0        would allocate: 16384   overhead: 0%
Payload size: 32768     extra: 0        would allocate: 32768   overhead: 0%
Payload size: 65536     extra: 0        would allocate: 65536   overhead: 0%
Payload size: 131072    extra: 0        would allocate: 131072  overhead: 0%
Payload size: 262144    extra: 0        would allocate: 262144  overhead: 0%
Payload size: 524288    extra: 0        would allocate: 524288  overhead: 0%
----------
Payload size: 4096      extra: 32       would allocate: 4736    overhead: 14.73%
Payload size: 8192      extra: 32       would allocate: 9472    overhead: 15.18%
Payload size: 16384     extra: 32       would allocate: 20480   overhead: 24.76%
Payload size: 32768     extra: 32       would allocate: 40960   overhead: 24.88%
Payload size: 65536     extra: 32       would allocate: 73728   overhead: 12.45%
Payload size: 131072    extra: 32       would allocate: 139264  overhead: 6.224%
Payload size: 262144    extra: 32       would allocate: 270336  overhead: 3.112%
Payload size: 524288    extra: 32       would allocate: 532480  overhead: 1.556%
----------

Reproducing...
Extra size: 32
Payload size: 32768
Payload count: 8192
Expected total usage MB: 256.2

generic.bytes_in_use_by_app diff MB: 320
total overhead: 24.88%
generic.heap_size diff MB: 320
generic.physical_memory_used diff MB: 320.8
tcmalloc.metadata_bytes diff MB: 0.5168
tcmalloc.page_heap_free diff MB: 0
tcmalloc.required_bytes diff MB: 320.8

```


tcmalloc with 256k logical pages
=====


```
Payload size: 4096      extra: 0        would allocate: 4096    overhead: 0%
Payload size: 8192      extra: 0        would allocate: 8192    overhead: 0%
Payload size: 16384     extra: 0        would allocate: 16384   overhead: 0%
Payload size: 32768     extra: 0        would allocate: 32768   overhead: 0%
Payload size: 65536     extra: 0        would allocate: 65536   overhead: 0%
Payload size: 131072    extra: 0        would allocate: 131072  overhead: 0%
Payload size: 262144    extra: 0        would allocate: 262144  overhead: 0%
Payload size: 524288    extra: 0        would allocate: 524288  overhead: 0%
----------
Payload size: 4096      extra: 32       would allocate: 4736    overhead: 14.73%
Payload size: 8192      extra: 32       would allocate: 8704    overhead: 5.837%
Payload size: 16384     extra: 32       would allocate: 17408   overhead: 6.043%
Payload size: 32768     extra: 32       would allocate: 37376   overhead: 13.95%
Payload size: 65536     extra: 32       would allocate: 74880   overhead: 14.2%
Payload size: 131072    extra: 32       would allocate: 149760  overhead: 14.23%
Payload size: 262144    extra: 32       would allocate: 524288  overhead: 99.98%
Payload size: 524288    extra: 32       would allocate: 786432  overhead: 49.99%
----------

Reproducing...
Extra size: 32
Payload size: 32768
Payload count: 8192
Expected total usage MB: 256.2

generic.bytes_in_use_by_app diff MB: 323.5
total overhead: 26.24%
generic.heap_size diff MB: 324
generic.physical_memory_used diff MB: 324.5
tcmalloc.metadata_bytes diff MB: 0.2098
tcmalloc.page_heap_free diff MB: 1.759e+13
tcmalloc.required_bytes diff MB: 324.8
```
