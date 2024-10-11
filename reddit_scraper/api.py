from dataclasses import dataclass, field
from enum import Enum
from http.cookiejar import DefaultCookiePolicy
from time import sleep, time
from typing import Any, ClassVar

from fake_useragent import UserAgent
from requests import Session, Timeout
from requests.exceptions import RetryError


class SubredditSort(Enum):
    HOT = "hot"
    NEW = "new"
    TOP = "top"
    CONTROVERSIAL = "controversial"
    RISING = "rising"


class SubredditT(Enum):
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"
    ALL = "all"


class CommentsSort(Enum):
    CONFIDENCE = "confidence"
    TOP = "top"
    NEW = "new"
    CONTROVERSIAL = "controversial"
    OLD = "old"
    QA = "qa"


@dataclass
class RedditAPI:
    BASE_URL: ClassVar[str] = "https://reddit.com"
    TIMEOUT: ClassVar[int] = 10
    MAX_TRIES: ClassVar[int] = 3

    session: Session = field(default_factory=Session, init=False, repr=False)
    user_agent: UserAgent = field(default_factory=UserAgent, init=False, repr=False)
    requests_remaining: int = field(default=1, init=False, repr=False)
    reset_time: float = field(default=0, init=False, repr=False)

    def __post_init__(self) -> None:
        self.session.headers = {"User-Agent": self.user_agent.random}
        self.session.cookies.set_policy(DefaultCookiePolicy(allowed_domains=[]))

    def get(self, endpoint: str, **params: str) -> Any:
        params["raw_json"] = "1"

        for _ in range(self.MAX_TRIES):
            now = time()

            if now > self.reset_time:
                self.requests_remaining = 1
            elif not self.requests_remaining:
                sleep(self.reset_time - now)
                self.session.headers["User-Agent"] = self.user_agent.random
                self.requests_remaining = 1

            try:
                response = self.session.get(f"{self.BASE_URL}{endpoint}.json", params=params, timeout=self.TIMEOUT)
            except Timeout:
                continue

            if response.status_code not in (200, 429):
                response.raise_for_status()

            now = time()
            self.requests_remaining = int(float(response.headers["X-Ratelimit-Remaining"]))
            self.reset_time = now + int(response.headers["X-Ratelimit-Reset"])

            if response.status_code == 429:
                sleep(max(self.TIMEOUT, self.reset_time - now))
                self.session.headers["User-Agent"] = self.user_agent.random
                continue

            return response.json()

        raise RetryError()

    def listing(self, endpoint: str, before: str | None, after: str | None, **params: str) -> Any:
        params["limit"] = "100"

        if before:
            params["before"] = before
        elif after:
            params["after"] = after

        return self.get(endpoint, **params)

    def r(
        self,
        subreddit: str,
        sort: SubredditSort = SubredditSort.HOT,
        t: SubredditT = SubredditT.DAY,
        before: str | None = None,
        after: str | None = None
    ) -> Any:
        endpoint = f"/r/{subreddit}/{sort.value}"

        if sort in (SubredditSort.TOP, SubredditSort.CONTROVERSIAL):
            return self.listing(endpoint, before, after, t=t.value)

        return self.listing(endpoint, before, after)

    def comments(self, article: str, sort: CommentsSort = CommentsSort.CONFIDENCE) -> Any:
        return self.get(f"/comments/{article}", sort=sort.value)
