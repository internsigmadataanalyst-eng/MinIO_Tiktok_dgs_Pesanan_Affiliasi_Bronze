import sys
from datetime import datetime

print(">>> main.py started")

from src.pesanan_affiliasi.pipelines.run_daily_etl import run_daily_etl
from src.pesanan_affiliasi.utils.log import (
    setup_run_logging,
    setup_event_logging,
    emit,
)

if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv or "-n" in sys.argv
    if dry_run:
        print(">>> dry-run mode enabled")

    run_key = datetime.now().strftime("%Y%m%d%H%M")

    # Only capture run transcripts for production runs, not dry-run
    if not dry_run:
        log_path, restore_logging = setup_run_logging(run_key)
        print(f">>> Log file: {log_path}")
    else:
        restore_logging = None

    # Structured JSON events: written for BOTH production and dry-run
    _, stop_event_logging = setup_event_logging(run_key, dry_run)

    try:
        print(f">>> run_key={run_key}")
        emit("PIPELINE", "main", f"ETL start (run_key={run_key})")
        print(f">>> calling run_daily_etl()")
        run_daily_etl(dry_run=dry_run)
        emit("PIPELINE", "main", "ETL Pipeline finished")
        print(">>> main.py finished")
    except Exception as e:
        emit(
            "PIPELINE", "main",
            f"Unhandled exception: {type(e).__name__} {e}",
            level="ERROR",
        )
        raise
    finally:
        stop_event_logging()
        if restore_logging:
            restore_logging()