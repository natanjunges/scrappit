# Scrappit, Simple Reddit Scraper
# Copyright (C) 2024  Natan Junges <natanajunges@gmail.com>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from dataclasses import dataclass, field
from enum import Enum
from http.cookiejar import DefaultCookiePolicy
from time import sleep, time
from typing import ClassVar

from fake_useragent import UserAgent
from requests import Session, Timeout
from requests.exceptions import RetryError

from .common import JSON


@dataclass
class RedditAPIItem:
    name: str
    priority: float


class RedditAPITask(Enum):
    GET = RedditAPIItem("get", 0 / 8)
    LISTING = RedditAPIItem("listing", 1 / 8)
    R_ABOUT = RedditAPIItem("r_about", 6 / 8)
    R = RedditAPIItem("r", 2 / 8)
    USER_ABOUT = RedditAPIItem("user_about", 7 / 8)
    USER = RedditAPIItem("user", 3 / 8)
    COMMENTS = RedditAPIItem("comments", 4 / 8)
    API_MORECHILDREN = RedditAPIItem("api_morechildren", 5 / 8)


class RedditAPISubredditSort(Enum):
    HOT = RedditAPIItem("hot", 2 / 5)
    NEW = RedditAPIItem("new", 0 / 5)
    TOP = RedditAPIItem("top", 4 / 5)
    CONTROVERSIAL = RedditAPIItem("controversial", 3 / 5)
    RISING = RedditAPIItem("rising", 1 / 5)


class RedditAPIT(Enum):
    HOUR = RedditAPIItem("hour", 0 / 6)
    DAY = RedditAPIItem("day", 1 / 6)
    WEEK = RedditAPIItem("week", 2 / 6)
    MONTH = RedditAPIItem("month", 3 / 6)
    YEAR = RedditAPIItem("year", 4 / 6)
    ALL = RedditAPIItem("all", 5 / 6)


class RedditAPIUserWhere(Enum):
    OVERVIEW = RedditAPIItem("overview", 0 / 3)
    SUBMITTED = RedditAPIItem("submitted", 2 / 3)
    COMMENTS = RedditAPIItem("comments", 1 / 3)


class RedditAPIUserSort(Enum):
    HOT = RedditAPIItem("hot", 1 / 4)
    NEW = RedditAPIItem("new", 0 / 4)
    TOP = RedditAPIItem("top", 3 / 4)
    CONTROVERSIAL = RedditAPIItem("controversial", 2 / 4)


class RedditAPICommentsSort(Enum):
    CONFIDENCE = RedditAPIItem("confidence", 1 / 6)
    TOP = RedditAPIItem("top", 4 / 6)
    NEW = RedditAPIItem("new", 0 / 6)
    CONTROVERSIAL = RedditAPIItem("controversial", 3 / 6)
    OLD = RedditAPIItem("old", 5 / 6)
    QA = RedditAPIItem("qa", 2 / 6)


@dataclass
class RedditAPI:
    BASE_URL: ClassVar[str] = "https://reddit.com"
    TIMEOUT: ClassVar[int] = 10
    MAX_TRIES: ClassVar[int] = 3

    session: Session = field(default_factory=Session, init=False, repr=False)
    user_agent: UserAgent = field(default_factory=UserAgent, init=False, repr=False)
    requests_remaining: bool = field(default=True, init=False, repr=False)
    reset_time: float = field(default=0, init=False, repr=False)

    def __post_init__(self) -> None:
        self.session.headers = {"User-Agent": self.user_agent.random}
        self.session.cookies.set_policy(DefaultCookiePolicy(allowed_domains=[]))

    def get(self, endpoint: str, **params: str) -> JSON:
        params["raw_json"] = "1"

        for _ in range(self.MAX_TRIES):
            now = time()

            if now > self.reset_time:
                self.requests_remaining = True
            elif not self.requests_remaining:
                sleep(self.reset_time - now)
                self.session.headers["User-Agent"] = self.user_agent.random
                self.requests_remaining = True

            try:
                response = self.session.get(f"{self.BASE_URL}{endpoint}.json", params=params, timeout=self.TIMEOUT)
            except Timeout:
                continue

            if response.status_code not in (200, 429):
                response.raise_for_status()

            now = time()
            self.requests_remaining = bool(float(response.headers["X-Ratelimit-Remaining"]))
            self.reset_time = now + int(response.headers["X-Ratelimit-Reset"])

            if response.status_code == 429:
                sleep(max(self.TIMEOUT, self.reset_time - now))
                self.session.headers["User-Agent"] = self.user_agent.random
                continue

            return response.json()

        raise RetryError()

    def listing(self, endpoint: str, before: str | None = None, after: str | None = None, **params: str) -> JSON:
        params["limit"] = "100"

        if before:
            params["before"] = before
        elif after:
            params["after"] = after

        return self.get(endpoint, **params)

    def r_about(self, subreddit: str) -> JSON:
        return self.get(f"/r/{subreddit}/about")

    def r(
        self,
        subreddit: str,
        sort: RedditAPISubredditSort = RedditAPISubredditSort.HOT,
        t: RedditAPIT = RedditAPIT.DAY,
        before: str | None = None,
        after: str | None = None
    ) -> JSON:
        endpoint = f"/r/{subreddit}/{sort.value.name}"

        if sort in (RedditAPISubredditSort.TOP, RedditAPISubredditSort.CONTROVERSIAL):
            return self.listing(endpoint, before, after, t=t.value.name)

        return self.listing(endpoint, before, after)

    def user_about(self, username: str) -> JSON:
        return self.get(f"/user/{username}/about")

    def user(
        self,
        username: str,
        where: RedditAPIUserWhere = RedditAPIUserWhere.OVERVIEW,
        sort: RedditAPIUserSort = RedditAPIUserSort.NEW,
        t: RedditAPIT = RedditAPIT.ALL,
        before: str | None = None,
        after: str | None = None
    ) -> JSON:
        endpoint = f"/user/{username}/{where.value.name}"

        if sort in (RedditAPIUserSort.TOP, RedditAPIUserSort.CONTROVERSIAL):
            return self.listing(endpoint, before, after, sort=sort.value.name, t=t.value.name)

        return self.listing(endpoint, before, after, sort=sort.value.name)

    def comments(self, article: str, sort: RedditAPICommentsSort = RedditAPICommentsSort.CONFIDENCE, comment: str | None = None) -> JSON:
        endpoint = f"/comments/{article}"

        if comment:
            return self.get(endpoint, sort=sort.value.name, comment=comment)

        return self.get(endpoint, sort=sort.value.name)

    def api_morechildren(self, link_id: str, children: list[str], sort: RedditAPICommentsSort = RedditAPICommentsSort.CONFIDENCE) -> JSON:
        return self.get("/api/morechildren", api_type="json", link_id=link_id, children=",".join(children), sort=sort.value.name)
