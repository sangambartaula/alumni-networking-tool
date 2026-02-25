import backend.sqlite_fallback as sqlite_fallback


def test_cleanup_ignores_logger_write_errors(monkeypatch):
    manager = sqlite_fallback.get_connection_manager()
    monkeypatch.setattr(
        sqlite_fallback.logger,
        "info",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("stream closed")),
    )

    # Should not raise even if logger stream is already closed.
    manager._cleanup()
