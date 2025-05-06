## Strawberry


Is released as a docker image.




**Releases:**

{% cut "**v0.0.13**" %}

**Release date:** 2025-04-08


### General Updates

**Features:**

- Improved support for Jupyter operations. Thanks to @dmi-feo and @thenno for the PRs!
- Added restart command to the controller. (Commit: 7eef7be610082c92ab7608fbebe58f64bf4db42d)
- Added location-based overriding of the strawberry config. (Commit: 53761978b238167d340a4c4f0ef8309a3555941d)
- Added the `pool_trees` speclet option. (Commit: 26a36552dcd47b4126e21b226532dd1b9c6c551a)
- Added a `filters` parameter to the `list` command. (Commit: 007a51cc8364fca85cb8c680b39265dffc76ceda)
- Added a config option to grant the `administer` right to the operation creator. (Commit: 40e9bff15e07d6128bd27b4e77963fe793312bd9)

**Fixes:**

- Fixed panic on initialization if the cluster is unavailable. (Commit: b567f1737aeadd3c1ee0eba0d7bc7b46ec66a789) 

### CHYT

- Added an option to disable the export of runtime data from CHYT operations and an explicit expiration timeout for exported data. (Commit: 58d91c249ee4aaf7d7be3070af58569f5f2ad1b9)
- Changed the default `read_from` option in AttributeCache. (Commit: 33de404dcce77593968fa45d548bf9ebceb3204e)

### SPYT

- Added Squashfs support for Livy via strawberry. (Commit: 15c08cd668eacdf57312dc9bcb01452faa82ce7d)
- Specified yt token for Livy operations. (Commit: c91680616715f65da1855669e082a914f9909973)

### Jupyter

- Added GPU support in Jupyter operations. (Commit: 935e0a5a7c2ffd4a45d3e4260f9aea9d4534a8c0)
- Added job scaler interface and used it to suspend inactive Jupyter operations. (Commit: e4b7df0c0213644f54af6411ed06b2bc34576059)

{% endcut %}


{% cut "**v0.0.12**" %}

**Release date:** 2024-06-21


**CHYT:**
- Make `enable_geodata`  default value configurable and set to `false` by default (PR: #667). Thanks @thenno for the PR!
- Configure system log tables exporter during the clique start

**Livy:**
- Add SPYT Livy support to the controller

{% endcut %}

