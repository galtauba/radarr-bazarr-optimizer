from pathlib import Path

from dotenv import load_dotenv

from optimizer_app.bootstrap import run_app


def main() -> None:
    project_root = Path(__file__).resolve().parent
    load_dotenv(project_root / ".env", override=False)
    run_app()


if __name__ == "__main__":
    main()
