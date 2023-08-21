# Digest

Digest functions calculate hashes (aka fingerprints) of original messages. A hash is usually a short or fixed-length string. Hash length depends on the algorithm. The result is a bit sequence.
The module includes both cryptographic and non-cryptographic functions.

An important feature of algorithms is that changing the original message also changes the hash output. Also, digest functions are one-way functions, meaning that it's hard to find a message that will produce a given digest.

**List of functions**

* ```Digest::Crc32c(String{Flags::AutoMap}) -> Uint32```
* ```Digest::Fnv32(String{Flags::AutoMap}) -> Uint32```
* ```Digest::Fnv64(String{Flags::AutoMap}) -> Uint64```
* ```Digest::MurMurHash(String{Flags:AutoMap}) -> Uint64```
* ```Digest::CityHash(String{Flags:AutoMap}) -> Uint64```
* ```Digest::CityHash128(String{Flags:AutoMap}) -> Tuple<Uint64,Uint64>```

CityHash is a byte string function that returns a uint128 value. The result is a pair of two uint64 numbers: <low, high>.
```sql
select Digest::CityHash128("Who set this ancient quarrel new abroach?"); -- (11765163929838407746,2460323961016211789)
```

* ```Digest::NumericHash(Uint64{Flags:AutoMap}) -> Uint64```

```sql
SELECT Digest::NumericHash(123456789); -- 1734215268924325803
```

* ```Digest::Md5Hex(String{Flags:AutoMap}) -> String```
* ```Digest::Md5Raw(String{Flags:AutoMap}) -> String```
* ```Digest::Md5HalfMix(String{Flags:AutoMap}) -> Uint64```

MD5 is a widely used hashing algorithm that results in a 128-bit hash value.

Md5Hex returns an ASCII string hash with hex encoding.

Md5Raw returns a hash as a byte string.

Md5HalfMix is a lightweight MD5 that returns a Uint64 value.

```sql
select
    Digest::Md5Hex("Who set this ancient quarrel new abroach?"), -- "644e98bae764871650f2d93e14c6488d"
    Digest::Md5Raw("Who set this ancient quarrel new abroach?"), -- Binary String: 64 4e 98 ba e7 64 87 16 50 f2 d9 3e 14 c6 48 8d
    Digest::Md5HalfMix("Who set this ancient quarrel new abroach?"); -- 17555822562955248004
```

* ```Digest::Argon2(string:String{Flags:AutoMap}, salt:String{Flags:AutoMap}) -> String```

Argon2 is a key derivation function aimed at a high memory-filling rate and good utilization of multiple computing units.

Parameters:
- string — the source string
- salt — the string to be used as the salt for the hash function
```sql
select Digest::Argon2("Who set this ancient quarrel new abroach?", "zcIvVcuHEIL8"); -- Binary String: fa 50 34 d3 c3 23 a4 de 22 c7 7c e1 9c 65 64 88 25 b3 59 75 c5 b8 8c 73 da 88 eb 79 31 70 e8 f1
select Digest::Argon2("Who set this ancient quarrel new abroach?", "M78P42R8HA=="); -- Binary String: d2 0e f1 3e 72 5a e9 32 65 ed 28 4b 12 1f 39 70 e5 10 aa 1a 15 67 6d 96 5d e8 19 b3 bd d5 04 e9
```
* ```Digest::Blake2B(string:String{Flags:AutoMap},[key:String?]) -> String```

BLAKE2 is a cryptographic hash function designed as an alternative to MD5 and SHA-1 algorithms, which have been shown to be vulnerable.

Parameters:
- string — the source string
- key is the key used to encrypt the source string (it can be used as a shared secret for the sender and receiver)
```sql
select Digest::Blake2B("Who set this ancient quarrel new abroach?"); -- Binary String: 62 21 91 d8 11 5a da ad 5e 7c 86 47 41 02 7f 8f a8 a6 82 07 47 d8 f8 30 ab b4 c3 00 db 9c 24 2f
```
* ```Digest::SipHash(low:Uint64,high:Uint64,string:String{Flags:AutoMap}) -> Uint64```

Hashes the source message (```string```) using a 128-bit key. The key is represented by a pair of uint64 numbers: low, high

```sql
select Digest::SipHash(0,0,"Who set this ancient quarrel new abroach?"); -- 14605466535756698285
```

* ```Digest::HighwayHash(key0:Uint64,key1:Uint64,key2:Uint64,key3:Uint64,string:String{Flags:AutoMap}) -> Uint64```

Hashes the source message (```string```) using a 256-bit key.

The key has the following structure:

    — key0 — first 8 bytes of the key
    — key1 — next 8 bytes of the key
    — key2 — subsequent 8 bytes of the key
    — key3 — last 8 bytes of the key

* ```Digest::FarmHashFingerprint(source:Uint64{Flags:AutoMap}) -> Uint64```
* ```Digest::FarmHashFingerprint2(low:Uint64{Flags:AutoMap}, high:Uint64{Flags:AutoMap}) -> Uint64```

FarmHash is a function for a 128-bit number. This 128-bit number is made by combining bits of two uit64 numbers.

* ```Digest::FarmHashFingerprint32(string:String{Flags:AutoMap}) -> Uint32```
* ```Digest::FarmHashFingerprint64(string:String{Flags:AutoMap}) -> Uint64```
* ```Digest::FarmHashFingerprint128(string:String{Flags:AutoMap}) -> Tuple<Uint64,Uint64>```

FarmHash is a byte string function that returns a uint128 value. The result is a pair of two uint64 numbers: <low, high>.

```sql
select Digest::FarmHashFingerprint2(31880,6990065); -- 237693065644851126
select Digest::FarmHashFingerprint128("Who set this ancient quarrel new abroach?"); -- (17165761740714960035, 5559728965407786337)
```

* ```Digest::SuperFastHash(String{Flags:AutoMap}) -> Uint32```
* ```Digest::Sha1(String{Flags:AutoMap}) -> String```
* ```Digest::Sha256(String{Flags:AutoMap}) -> String```
* ```Digest::IntHash64(Uint64{Flags:AutoMap}) -> Uint64```

* ```Digest::XXH3(String{Flags:AutoMap}) -> Uint64```
* ```Digest::XXH3_128(String{Flags:AutoMap}) -> Tuple<Uint64,Uint64>```

XXH3 is a non-cryptographic hash function that belongs to the xxxHash family. XXH3_128 generates a 128-bit hash represented by a pair of two uint64 numbers: <low, high>
```sql
select Digest::XXH3_128("Who set this ancient quarrel new abroach?"); -- (17117571879768798812, 14282600258804776266)
```
