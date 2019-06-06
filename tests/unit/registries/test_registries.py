import pytest

from tentaclio.registries.registry import URLHandlerRegistry


class TestRegistry(object):
    def test_register_handler(self, fake_handler):
        registry = URLHandlerRegistry()
        registry.register("scheme", fake_handler)
        result = registry.get_handler("scheme")
        assert fake_handler is result

    def test_unknown_handler(self):
        registry = URLHandlerRegistry()
        with pytest.raises(KeyError):
            registry.get_handler("scheme")

    def test_contains_handler(self, fake_handler):
        registry = URLHandlerRegistry()
        registry.register("http", fake_handler)
        assert "http" in registry
        assert "https" not in registry
