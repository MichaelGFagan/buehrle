"""Regression guard for the Textual shell (loaders/interactive/app.py).

app.py is coverage-omitted (untestable UI), but this one check protects against
a specific, silent failure mode: shadowing a Textual-internal attribute on a
screen so its message pump never starts and it never mounts. That's what made
`RunScreen` freeze (it set ``self._running``, MessagePump's loop flag).

Empty-job ``RunScreen`` is used so no subprocess spawns: the worker writes its
summary line and finishes immediately, keeping the harness deterministic.
"""

import asyncio

from loaders.interactive.app import BuehrleApp, RunScreen
from loaders.state import DEFAULT_DB


async def _push_and_settle(screen):
    app = BuehrleApp(DEFAULT_DB)
    async with app.run_test() as pilot:
        await app.push_screen(screen)
        for _ in range(20):
            try:
                await asyncio.wait_for(pilot.pause(), timeout=0.5)
                break
            except asyncio.TimeoutError:
                pass
        mounted = screen.is_mounted
        child_count = len(screen.children)
        app.exit(0)
    return mounted, child_count


def test_run_screen_mounts():
    # With the `_running` bug this never mounts (compose/on_mount never fire).
    screen = RunScreen([])
    mounted, child_count = asyncio.run(_push_and_settle(screen))
    assert mounted is True
    assert child_count > 0          # Header + Log + Footer composed
