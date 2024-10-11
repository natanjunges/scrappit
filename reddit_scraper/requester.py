from dataclasses import dataclass, field
from http.cookiejar import DefaultCookiePolicy
from time import sleep, time
from typing import Any, ClassVar

from requests import Session, Timeout
from requests.exceptions import RetryError


@dataclass
class RedditRequester:
    USER_AGENT: ClassVar[str] = "Mozilla/5.0 (X11; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0"
    BASE_URL: ClassVar[str] = "https://reddit.com"
    TIMEOUT: ClassVar[int] = 10
    MAX_TRIES: ClassVar[int] = 3

    session: Session = field(default_factory=Session, init=False, repr=False)
    requests_remaining: int = field(default=1, init=False, repr=False)
    reset_time: float = field(default=0, init=False, repr=False)

    def __post_init__(self) -> None:
        self.session.headers = {"User-Agent": self.USER_AGENT}
        self.session.cookies.set_policy(DefaultCookiePolicy(allowed_domains=[]))

    def get(self, endpoint: str) -> Any:
        for _ in range(self.MAX_TRIES):
            now = time()

            if now > self.reset_time:
                self.requests_remaining = 1
            elif not self.requests_remaining:
                sleep(self.reset_time - now)
                self.requests_remaining = 1

            try:
                response = self.session.get(f"{self.BASE_URL}{endpoint}", timeout=self.TIMEOUT)
            except Timeout:
                continue

            if response.status_code not in (200, 429):
                response.raise_for_status()

            now = time()
            self.requests_remaining = int(float(response.headers["X-Ratelimit-Remaining"]))
            self.reset_time = now + int(response.headers["X-Ratelimit-Reset"])

            if response.status_code == 429:
                sleep(min(self.TIMEOUT, self.reset_time - now))
                continue

            return response.json()

        raise RetryError()
