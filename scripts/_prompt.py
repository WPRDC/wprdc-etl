"""Reusable interactive prompts for wprdc-etl scripts.

Arrow-key menus, text input, and confirms, built on `questionary`. Import these
instead of using input() directly so every script feels the same:

    from _prompt import select, text, confirm

Install the dependency once:  uv add questionary
"""

from __future__ import annotations

import sys

try:
    import questionary
    from questionary import Choice
except ImportError:  # pragma: no cover
    sys.stderr.write(
        "This script needs 'questionary'.\n"
        "  uv add questionary          (into the project)\n"
        "  uv pip install questionary  (ad hoc)\n"
    )
    sys.exit(1)


def _ask(question):
    """Run a questionary prompt; treat Esc/Ctrl-C (which return None) as abort."""
    answer = question.ask()
    if answer is None:
        sys.stderr.write("\naborted.\n")
        sys.exit(1)
    return answer


def select(message, choices, default=None):
    """Arrow-key single-select. `choices` items are either a plain string, or a
    (value, description) tuple rendered as 'value — description'. Returns the
    chosen value. `default` is the value to preselect."""
    built = []
    for c in choices:
        if isinstance(c, tuple):
            value, desc = c
            built.append(Choice(title=f"{value} — {desc}", value=value))
        else:
            built.append(c)
    return _ask(questionary.select(message, choices=built, default=default))


def text(message, default="", required=False):
    """Free-text input. When required, empties are rejected inline."""
    validate = (lambda v: True if v.strip() else "required") if required else None
    return _ask(questionary.text(message, default=default, validate=validate)).strip()


def confirm(message, default=False):
    """Yes/no confirm."""
    return _ask(questionary.confirm(message, default=default))
