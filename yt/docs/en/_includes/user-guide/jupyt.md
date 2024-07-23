---
title: Jupyter Notebooks | {{product-name}}
---

# Using Jupyter Notebooks in {{product-name}}

{{product-name}} allows to launch Jupyter Notebooks on the computational resources of the cluster. Functionality can be used on clusters using CRI job environment.

Following actions are required to launch a notebook.

1. Set the environment variable `JUPYT_CTL_ADDRESS` to the address of JUPYT strawberry controller.
```bash
export JUPYT_CTL_ADDRESS=jupyt.test.yt.mycloud.net
```

1. Select docker-image of the jupyter-notebook. You can use [jupyter stacks](https://jupyter-docker-stacks.readthedocs.io/en/latest/) as a image or as a base of the image. Example of such an image is [minimal-notebook](quay.io/jupyter/minimal-notebook).

1. Create a notebook. To create a notebook you should specify docker-image with the notebook and scheduler pool for the operation.
```bash
yt jupyt ctl create --speclet-options '{jupyter_docker_image="quay.io/jupyter/minimal-notebook"; pool=my-pool}' test-jupyt
```

1. Launch the notebook.
```bash
yt jupyt ctl start test-jupyt
```

1. Wait until notebook is started and get the link to it using CLI.
```bash
foo@bar:~$ yt jupyt ctl get-endpoint test-jupyt
{
    "address" = "http://some-node.yt.mycloud.net:27042";
    "operation_id" = "60b8ec86-6f123a7d-134403e8-ee6b88de";
    "job_id" = "16f76ac9-2a20ab96-13440384-14e";
}
```

{% note warning "Warning" %}

Notebooks and files created on filesystem are not preserved after job restart. For the reliable storage of the files you should store them in Cypress.

{% endnote %}
