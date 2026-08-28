"""Sentinel entrypoint — loads gzip+base64 split source (GitHub size limit)."""
import base64
import gzip
from pathlib import Path

_dir = Path(__file__).resolve().parent
_parts = sorted(_dir.glob("_gz_*.txt"), key=lambda p: int(p.stem.split("_")[-1]))
if not _parts:
    raise SystemExit("missing _gz_*.txt parts next to bot.py")
_b64 = "".join(p.read_text(encoding="ascii").strip() for p in _parts)
_src = gzip.decompress(base64.b64decode(_b64)).decode("utf-8")
exec(compile(_src, str(_dir / "bot_impl.py"), "exec"), globals())
