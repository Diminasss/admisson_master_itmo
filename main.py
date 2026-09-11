from LoadSubmittedDocuments import loadSubmittedDocuments
import asyncio
import logging

# Конфиг)
LIST_OF_ADMISSION_SOURCE_LINKS: str = "https://abit.itmo.ru/ratings/master"
LIST_OF_ADMISSION_TARGET_DIR: str = "ListsOfSubmittedDocuments"


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    result = asyncio.run(loadSubmittedDocuments(
        LIST_OF_ADMISSION_SOURCE_LINKS,
        LIST_OF_ADMISSION_TARGET_DIR
    ))
    print(f"Скачано: {len(result.saved)}. Ошибок: {len(result.failed)}.")
    print(f"Отчёт: {LIST_OF_ADMISSION_TARGET_DIR}/download_report.json")
    return 1 if result.failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
