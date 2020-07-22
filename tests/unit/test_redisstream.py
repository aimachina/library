import pytest

from events import redisstream


class RedisDummy:
    def xack(self, *args, **kwargs):
        pass


class MockEvent:
    event_type = "some-type"

    def update_correlations(self, *args, **kwargs):
        return "updated correlations"


@pytest.fixture
def captured(capsys):
    def _captured():
        return capsys.readouterr().out.strip()

    return _captured


def test_correlations_context_is_copied(monkeypatch, captured):
    def digest_spy(*args, **kwargs):
        redisstream.correlations_context.set("context has been copied")
        c = redisstream.correlations_context.get()
        print(c)

    monkeypatch.setattr(redisstream, "digest_event", digest_spy)
    monkeypatch.setattr(redisstream, "decode_item", lambda *a, **k: ("some-stream", [None], [None]))
    redisstream.decode_and_digest(RedisDummy(), None, "test-group", {})
    assert not redisstream.correlations_context.get() and captured() == "context has been copied"


def test_correlations_context_is_set(monkeypatch, captured):
    def handler_spy(*args, **kwargs):
        c = redisstream.correlations_context.get()
        print(c)

    redisstream.digest_event("", MockEvent(), "", {"some-type": handler_spy})
    assert captured() == "updated correlations"
