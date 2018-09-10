import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="yandex_yt_job_plots",
    version="0.0.9",
    author="ivanashevi",
    description="Package for plotting job statistics.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    install_requires=["yandex-yt", "yandex-yt-yson-bindings", "numpy", "plotly"],
    python_requires='>=2.7',
)