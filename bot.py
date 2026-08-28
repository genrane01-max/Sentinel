"""Sentinel entrypoint — loads full bot source from split parts (GitHub push size limit)."""
from pathlib import Path

_dir = Path(__file__).resolve().parent
_parts = sorted(_dir.glob("_bot_src_*.txt"), key=lambda p: int(p.stem.split("_")[-1]))
if not _parts:
    raise SystemExit("missing _bot_src_*.txt parts next to bot.py")
_src = "".join(p.read_text(encoding="utf-8") for p in _parts)
exec(compile(_src, str(_dir / "bot_impl.py"), "exec"), globals())
