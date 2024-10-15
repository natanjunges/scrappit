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
from threading import Event, Thread
from time import sleep
from queue import Empty, PriorityQueue, Queue

from .api import JSON, SubredditSort, SubredditT, UserWhere, UserSort, UserT, CommentsSort, RedditAPI


@dataclass
class RedditAPITask:
    priority: int
    method: str = field(compare=False)
    args: tuple = field(compare=False)
    params: dict[str, str] = field(default_factory=dict, compare=False)


@dataclass
class RedditAPIResult:
    task: RedditAPITask
    value: JSON | Exception


class RedditAPIDispatcher(Thread):
    IDLE_SLEEP: float = 1 / 60

    def __init__(self) -> None:
        super().__init__()
        self.api: RedditAPI = RedditAPI()
        self.task_queue: PriorityQueue[RedditAPITask] = PriorityQueue()
        self.result_queue: Queue[RedditAPIResult] = Queue()
        self.running: Event = Event()

    def run(self) -> None:
        self.running.set()

        while self.running.is_set():
            if not self.task_queue.empty():
                task = self.task_queue.get()

                try:
                    json = getattr(self.api, task.method)(*task.args, **task.params)
                    self.result_queue.put(RedditAPIResult(task, json))
                except Exception as e:
                    self.result_queue.put(RedditAPIResult(task, e))

                self.task_queue.task_done()
            else:
                sleep(self.IDLE_SLEEP)

    def stop(self) -> None:
        self.running.clear()

    def put_task(self, task: RedditAPITask) -> None:
        self.task_queue.put(task)

    def get_result(self) -> RedditAPIResult | None:
        try:
            result = self.result_queue.get_nowait()
            self.result_queue.task_done()
            return result
        except Empty:
            return None

    def get(self, priority: int, endpoint: str, **params: str) -> None:
        self.put_task(RedditAPITask(priority, "get", (endpoint,), params))

    def listing(self, priority: int, endpoint: str, before: str | None = None, after: str | None = None, **params: str) -> None:
        self.put_task(RedditAPITask(priority, "listing", (endpoint, before, after), params))

    def r_about(self, priority: int, subreddit: str) -> None:
        self.put_task(RedditAPITask(priority, "r_about", (subreddit,)))

    def r(
        self,
        priority: int,
        subreddit: str,
        sort: SubredditSort = SubredditSort.HOT,
        t: SubredditT = SubredditT.DAY,
        before: str | None = None,
        after: str | None = None
    ) -> None:
        self.put_task(RedditAPITask(priority, "r", (subreddit, sort, t, before, after)))

    def user_about(self, priority: int, username: str) -> None:
        self.put_task(RedditAPITask(priority, "user_about", (username,)))

    def user(
        self,
        priority: int,
        username: str,
        where: UserWhere = UserWhere.OVERVIEW,
        sort: UserSort = UserSort.NEW,
        t: UserT = UserT.ALL,
        before: str | None = None,
        after: str | None = None
    ) -> None:
        self.put_task(RedditAPITask(priority, "user", (username, where, sort, t, before, after)))

    def comments(self, priority: int, article: str, sort: CommentsSort = CommentsSort.CONFIDENCE, comment: str | None = None) -> None:
        self.put_task(RedditAPITask(priority, "comments", (article, sort, comment)))

    def api_morechildren(self, priority: int, link_id: str, children: list[str], sort: CommentsSort = CommentsSort.CONFIDENCE) -> None:
        self.put_task(RedditAPITask(priority, "api_morechildren", (link_id, children, sort)))
