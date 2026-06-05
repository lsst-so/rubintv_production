import contextlib
import logging
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from typing import TextIO


@dataclass
class TestScript:
    path: str
    args: list[str] | None = None
    delay: float = 0.0
    display_on_pass: bool = False
    tee_output: bool = False
    do_debug: bool = False

    def __post_init__(self) -> None:
        if self.args is None:
            self.args = []
        if self.tee_output is False and self.do_debug is True:
            print("INFO: Enabling debug mode requires tee_output to be True. Forcing tee_output to True.")
            self.tee_output = True  # this is required for redirection to work

    def __str__(self) -> str:
        args_str = ":".join(self.args) if self.args else ""
        return f"{self.path}:{args_str}{'+debug' if self.do_debug else ''}"

    def __repr__(self) -> str:
        return str(self)

    def __hash__(self) -> int:
        return hash((self.path, tuple(self.args) if self.args else ()))

    @classmethod
    def from_existing(cls, existing: "TestScript", new_path: str) -> "TestScript":
        return cls(
            path=new_path,
            args=existing.args,
            delay=existing.delay,
            display_on_pass=existing.display_on_pass,
            tee_output=existing.tee_output,
            do_debug=existing.do_debug,
        )


@dataclass
class Check:
    passed: bool | None
    message: str

    def __str__(self) -> str:
        if self.passed is None:
            return f"⚠️  {self.message}"  # double space is needed for the unicode triangle
        return f"{'✅' if self.passed else '❌'} {self.message}"


class Tee:
    def __init__(self, *files: TextIO) -> None:
        self.files = files

    def write(self, obj: str) -> None:
        for f in self.files:
            f.write(obj)
            f.flush()  # Ensure immediate output

    def flush(self) -> None:
        for f in self.files:
            f.flush()

    def fileno(self) -> int:
        # Return the file descriptor of the first file that has one
        for f in self.files:
            if hasattr(f, "fileno"):
                return f.fileno()
        # If none of the files has a fileno method, raise
        raise IOError("This Tee instance doesn't have a valid file descriptor")

    def isatty(self) -> bool:
        # Check if any file is connected to a tty
        for f in self.files:
            if hasattr(f, "isatty") and f.isatty():
                return True
        return False

    def close(self) -> None:
        # Close all files
        for f in self.files:
            if hasattr(f, "close"):
                f.close()

    def writable(self) -> bool:
        # We're a stdout replacement so we should be writable
        return True

    def readable(self) -> bool:
        # Stdout is typically not readable
        return False

    def seekable(self) -> bool:
        # Stdout is typically not seekable
        return False


class LoggingTee(logging.Handler):
    def __init__(self, *handlers: logging.Handler) -> None:
        super().__init__()
        self.handlers = handlers

    def emit(self, record: logging.LogRecord) -> None:
        for handler in self.handlers:
            handler.emit(record)


@contextlib.contextmanager
def conditional_redirect(
    tee_output: bool,
    f_stdout: TextIO,
    f_stderr: TextIO,
    log_handler: logging.Handler,
    root_logger: logging.Logger,
) -> Iterator[None]:
    if tee_output:
        stdout = sys.stdout
        stderr = sys.stderr

        # Save existing handlers and remove them to prevent duplication
        existing_handlers = root_logger.handlers[:]
        for handler in existing_handlers:
            root_logger.removeHandler(handler)

        sys.stdout = Tee(stdout, f_stdout)
        sys.stderr = Tee(stderr, f_stderr)

        # Console handler should write to original stdout, not the tee
        console_handler = logging.StreamHandler(stdout)
        log_handler = LoggingTee(log_handler, console_handler)
        root_logger.addHandler(log_handler)
        try:
            yield
        finally:
            sys.stdout = stdout
            sys.stderr = stderr
            root_logger.removeHandler(log_handler)
            # Restore original handlers
            for handler in existing_handlers:
                root_logger.addHandler(handler)
    else:
        with contextlib.redirect_stdout(f_stdout), contextlib.redirect_stderr(f_stderr):
            yield
