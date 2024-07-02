variable "REPO_PATH" {}

variable "BUILD_ROOT" {
  default = "${REPO_PATH}/yt/docker"
}


# Common targets

target "python_binary_libraries" {
  args = {
    BUILD_TARGETS = "yson_lib driver_lib driver_rpc_lib"
  }
  contexts = {
    base_img = "docker-image://ubuntu:jammy"
  }

  dockerfile = "${BUILD_ROOT}/bake/built_yt.Dockerfile"
  context = "${REPO_PATH}"
}

target "yt_all" {
  args = {
    BUILD_TARGETS = ""
  }
  contexts = {
    base_img = "docker-image://ubuntu:jammy"
  }

  dockerfile = "${BUILD_ROOT}/bake/built_yt.Dockerfile"
  context = "${REPO_PATH}"
}

# Odin targets

target "odin_prepared_python_libs" {
  contexts = {
    python_binary_libraries = "target:python_binary_libraries"
  }
  target = "odin_prepared_python_libs"
  context = "${REPO_PATH}"
  dockerfile = "${BUILD_ROOT}/bake/odin.Dockerfile"
}

target "odin_libs" {
  contexts = {
    odin_prepared_python_libs = "target:odin_prepared_python_libs"
  }
  target = "odin_libs"
  context = "${REPO_PATH}"
  dockerfile = "${BUILD_ROOT}/bake/odin.Dockerfile"
}

target "odin" {
  contexts = {
    built_odin = "target:odin_libs"
  }
  target = "odin"
  context = "${REPO_PATH}"
  dockerfile = "${BUILD_ROOT}/bake/odin.Dockerfile"
}

# Python docker test image

target "python_test_image" {
  contexts = {
    yt_all = "target:yt_all"
  }
  context = "${REPO_PATH}"
  dockerfile = "${BUILD_ROOT}/bake/py_test_image.Dockerfile"
}
