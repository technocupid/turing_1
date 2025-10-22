import csv
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

def make_csv_writer(path, header):
    f = open(path, "w", newline="", encoding="utf-8")
    writer = csv.writer(f)
    writer.writerow(header)
    f.flush()

    def write_row(row):
        try:
            writer.writerow(row)
            f.flush()
        except Exception:
            logging.exception("Failed to write CSV row: %s", row)

    def close():
        try:
            f.close()
        except Exception:
            pass

    return write_row, close


def save_text(path, text):
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(text or "")
    except Exception:
        logging.exception("Failed to save text file: %s", path)


def save_binary(path, data):
    try:
        with open(path, "wb") as f:
            f.write(data)
    except Exception:
        logging.exception("Failed to save binary file: %s", path)
