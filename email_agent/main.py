"""Main entry point for the Email Agent.

Provides the CLI interface using Typer.
"""

import sys
from email_agent.cli import app


def main() -> int:
    """Main entry point for the email agent CLI.

    Returns:
        Exit code (0 for success, 1 for error).
    """
    try:
        app()
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
