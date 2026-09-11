import asyncio
import json
import logging
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlsplit

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class DownloadResult:
    saved: list[Path]
    failed: dict[str, str]


def _save_bytes(path: Path, content: bytes) -> None:
    temporary = path.with_suffix(path.suffix + ".part")
    try:
        temporary.write_bytes(content)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


class _RatingLinksParser(HTMLParser):
    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.links: dict[str, None] = {}

    def handle_starttag(self, tag, attrs):
        href = dict(attrs).get("href") if tag == "a" else None
        if not href:
            return
        parts = urlsplit(urljoin(self.base_url, href))
        if (
            parts.netloc == urlsplit(self.base_url).netloc
            and re.fullmatch(r"/(?:en/)?rating/\w+/[\w-]+/\d+/?", parts.path)
        ):
            self.links[parts._replace(query="", fragment="", path=parts.path.rstrip("/")).geturl()] = None


async def loadSubmittedDocuments(
    target_link: str,
    output_dir: str | Path,
    *,
    concurrency: int = 2,
    attempts: int = 2,
    retry_delay: float = 5.0,
) -> DownloadResult:
    """Скачать HTML списков и вернуть saved (пути) и failed (URL: ошибка).

    Для страницы ИТМО /ratings/master ссылки бюджета и контракта берутся
    из API, которое использует сайт. Повторные ссылки исключаются.
    Существующие файлы обновляются только после успешного скачивания.
    Сетевые ошибки, HTTP 429 и 5xx повторяются до attempts раз.
    Ошибка одной страницы не отменяет остальные; итог пишется в download_report.json.
    Ошибки получения исходного списка передаются вызывающему коду.
    """
    if concurrency < 1:
        raise ValueError("concurrency должен быть больше нуля")
    if attempts < 1 or retry_delay < 0:
        raise ValueError("attempts должен быть больше нуля, retry_delay — неотрицательным")
    if urlsplit(target_link).scheme not in {"http", "https"}:
        raise ValueError("target_link должен быть HTTP(S)-ссылкой")

    directory = Path(output_dir)
    timeout = aiohttp.ClientTimeout(total=60)
    connector = aiohttp.TCPConnector(limit=concurrency)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        semaphore = asyncio.Semaphore(concurrency)

        async def fetch(url: str, **kwargs) -> tuple[bytes, str]:
            for attempt in range(1, attempts + 1):
                delay = retry_delay * 2 ** (attempt - 1)
                try:
                    async with semaphore:
                        async with session.get(url, **kwargs) as response:
                            retry_after = response.headers.get("Retry-After", "")
                            if retry_after.isdecimal():
                                delay = max(delay, min(float(retry_after), 60))
                            response.raise_for_status()
                            return await response.read(), str(response.url)
                except (aiohttp.ClientError, TimeoutError) as error:
                    if isinstance(error, aiohttp.ClientResponseError):
                        if error.status not in {408, 429} and error.status < 500:
                            raise
                    if attempt == attempts:
                        raise
                    logger.warning("Повтор %s/%s через %.1f с: %s (%s)", attempt + 1, attempts, delay, url, error)
                    await asyncio.sleep(delay)
            raise AssertionError("Недостижимая ветка")

        content, base_url = await fetch(target_link)
        html = content.decode("utf-8-sig")

        parser = _RatingLinksParser(base_url)
        parser.feed(html)
        links = parser.links
        source = urlsplit(base_url)
        if source.hostname == "abit.itmo.ru" and source.path.rstrip("/") in {
            "/ratings/master", "/en/ratings/master"
        }:
            params = {"degree": "master"}
            title = parse_qs(source.query).get("title")
            if title:
                params["title"] = title[0]
            content, _ = await fetch(
                "https://abitlk.itmo.ru/api/v1/rating/directions",
                params=params,
                headers={"Accept-Language": "en" if source.path.startswith("/en/") else "ru"},
            )
            data = json.loads(content)
            prefix = "/en" if source.path.startswith("/en/") else ""
            for item in data["result"]["items"]:
                group_id = str(item["competitive_group_id"])
                if not group_id.isdecimal():
                    raise ValueError(f"Некорректный ID конкурсной группы: {group_id}")
                for financing in ("budget", "contract"):
                    links[urljoin(base_url, f"{prefix}/rating/master/{financing}/{group_id}")] = None

        if not links:
            raise ValueError(f"На странице не найдено ссылок на списки: {target_link}")

        directory.mkdir(parents=True, exist_ok=True)
        result = DownloadResult(saved=[], failed={})
        logger.info("Найдено страниц: %s", len(links))

        async def download(url: str) -> None:
            try:
                content, _ = await fetch(url)
                name = urlsplit(url).path.strip("/").replace("/", "_")
                path = directory / f"{name}.html"
                await asyncio.to_thread(_save_bytes, path, content)
                result.saved.append(path)
                logger.info("Сохранено %s/%s: %s", len(result.saved), len(links), path.name)
            except (aiohttp.ClientError, TimeoutError, OSError) as error:
                result.failed[url] = str(error)
                logger.error("Не удалось скачать %s: %s", url, error)

        async with asyncio.TaskGroup() as group:
            for url in links:
                group.create_task(download(url))
        result.saved.sort()
        report = {"source": target_link, "total": len(links),
                  "saved": [str(path) for path in result.saved], "failed": result.failed}
        await asyncio.to_thread(
            _save_bytes, directory / "download_report.json",
            json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8"),
        )
        return result

