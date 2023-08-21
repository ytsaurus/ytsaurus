# Porto layers

This section contains information about Porto layers.

## Terminology

- A **Porto layer** is a fragment of an image of a root file system for [Porto](https://github.com/yandex/porto);
- A **RootFS Image** is an image of a root file system;
- A **base layer**(**base image**) is an autonomous layer that contains the main part of the ambient scene (/bin/bash, libc6, etc.); by selecting a base layer, you are confirming the Ubuntu version (Precise, Trusty, Xenial, Bionic, etc.) your code will be working in.
- A **Delta layer** is a layer that supplements the base image and usually contains additional libraries or binary artifacts.

## Using layers to launch operations in {{product-name}}

A MapReduce operation must be launched in Porto layers if its execution requires libraries or resources that aren't available in {{product-name}} clusters, and if there is no way to prepare a binary file with this operation.

To use Porto layers, in the operation specifications in the mapper or reducer section, you must indicate  `layer_paths = ["//path/to/upper_layer"; ... ; "//path/to/lower_layer"]`. Layers are listed from top to bottom.

Standard and cached images are stored in {{product-name}} clusters. Using a task in Sandbox, you can also create layers with libraries and upload them right to [Cypress](../../../../user-guide/storage/cypress.md), along with the dependent images.
