from __future__ import annotations


def combined_output(stdout: str, stderr: str) -> str:
    return f"{stdout}\n{stderr}".strip()
