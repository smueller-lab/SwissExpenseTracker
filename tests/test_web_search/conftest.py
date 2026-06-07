from __future__ import annotations

# vcrpy 8.1.1 imports aiohttp.streams.AsyncStreamReaderMixin, which was removed
# in aiohttp 3.12+.  Since these tests play back pre-recorded cassettes and never
# issue real aiohttp requests, adding a no-op shim is enough for the import to
# succeed without affecting test behaviour.
import aiohttp.streams

if not hasattr(aiohttp.streams, "AsyncStreamReaderMixin"):
    aiohttp.streams.AsyncStreamReaderMixin = object  # type: ignore[attr-defined]
