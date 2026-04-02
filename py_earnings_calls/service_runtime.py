from __future__ import annotations

import json

import click
import uvicorn

from py_earnings_calls.api.app import create_app
from py_earnings_calls.config import load_config


@click.group()
def main() -> None:
    """Runtime wrapper."""


@main.command("api")
@click.option("--host", default="0.0.0.0", show_default=True)
@click.option("--port", default=8000, show_default=True, type=int)
def api(host: str, port: int) -> None:
    config = load_config()
    click.echo(json.dumps({
        "service": "api",
        "host": host,
        "port": port,
        "project_root": str(config.project_root),
    }, sort_keys=True))
    uvicorn.run(create_app(config), host=host, port=port)


if __name__ == "__main__":
    main()
