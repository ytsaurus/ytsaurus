from yt.wrapper.strawberry import StrawberryClient


class ChytClient(StrawberryClient):
    def __init__(self, **kwargs):
        super(ChytClient, self).__init__(family="chyt", **kwargs)
