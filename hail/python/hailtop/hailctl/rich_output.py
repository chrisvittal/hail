import os
from typing import IO, Any, Optional

import rich
from rich.console import Console


def print(
    *objects: Any,
    sep: str = " ",
    end: str = "\n",
    file: Optional[IO[str]] = None,
    flush: bool = False,
) -> None:
    is_test = bool(os.environ.get('HAIL_TEST'))
    write_console = rich.get_console() if file is None else Console(file=file)
    return write_console.print(*objects, sep=sep, end=end, soft_wrap=is_test)
