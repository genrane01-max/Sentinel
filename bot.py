"""Sentinel — reconstructs bot source from base64 parts."""
import base64
from pathlib import Path
_dir = Path(__file__).resolve().parent
_parts = sorted(_dir.glob("_b64_*.txt"), key=lambda p: int(p.stem.split("_")[-1]))
if not _parts:
    raise SystemExit("missing _b64_*.txt next to bot.py")
_src = base64.b64decode("".join(p.read_text() for p in _parts)).decode("utf-8")
exec(compile(_src, str(_dir / "bot_impl.py"), "exec"), globals())
