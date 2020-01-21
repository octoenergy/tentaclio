from tentaclio.fs.remover import ClientRemover
from tentaclio.urls import URL


class FakeRemover:
    def __init__(self, url):
        self.removed = False

    def remove(self) -> bool:
        self.removed = True
        return self.removed

    def __enter__(self) -> "FakeRemover":
        self.entered = True
        return self

    def __exit__(self, *args):
        pass


def test_client_remover():
    remover: FakeRemover

    def _fn(url):
        nonlocal remover
        remover = FakeRemover(url)
        return remover

    ClientRemover(_fn).remove(URL("fake://url"))
    assert remover.removed
