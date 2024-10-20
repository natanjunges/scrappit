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
from queue import Empty, PriorityQueue, Queue
from threading import Event, Thread
from time import sleep

from .common import ScrappitTask, ScrappitResult
from .api import SubredditSort, SubredditT, UserWhere, UserSort, UserT, CommentsSort, RedditAPI


@dataclass
class ScrappitDispatcherTask(ScrappitTask):
    priority: int = 0
    task_id: int = field(default=0, init=False, repr=False)


class ScrappitDispatcher(Thread):
    IDLE_SLEEP: float = 1 / 60

    def __init__(self) -> None:
        super().__init__()
        self.api: RedditAPI = RedditAPI()
        self.task_queue: PriorityQueue[ScrappitDispatcherTask] = PriorityQueue()
        self.task_id: int = 0
        self.result_queue: Queue[ScrappitResult] = Queue()
        self.running: Event = Event()

    def run(self) -> None:
        self.running.set()

        while self.running.is_set():
            if not self.task_queue.empty():
                task = self.task_queue.get()

                try:
                    json = getattr(self.api, task.task)(*task.args, **task.kwargs)
                    self.result_queue.put(ScrappitResult(task, json))
                except Exception as e:
                    self.result_queue.put(ScrappitResult(task, e))

                self.task_queue.task_done()
            else:
                sleep(self.IDLE_SLEEP)

    def stop(self) -> None:
        self.running.clear()

    def put_task(self, task: ScrappitDispatcherTask) -> ScrappitDispatcherTask:
        task.task_id = self.task_id
        self.task_id += 1
        self.task_queue.put(task)
        return task

    def get_result(self) -> ScrappitResult | None:
        try:
            result = self.result_queue.get_nowait()
            self.result_queue.task_done()
            return result
        except Empty:
            return None

    def get(self, priority: int, endpoint: str, **params: str) -> ScrappitDispatcherTask:
        return self.put_task(ScrappitDispatcherTask("get", (endpoint,), params, priority))

    def listing(self, priority: int, endpoint: str, before: str | None = None, after: str | None = None, **params: str) -> ScrappitDispatcherTask:
        return self.put_task(ScrappitDispatcherTask("listing", (endpoint, before, after), params, priority))

    def r_about(self, priority: int, subreddit: str) -> ScrappitDispatcherTask:
        return self.put_task(ScrappitDispatcherTask("r_about", (subreddit,), priority=priority))

    def r(
        self,
        priority: int,
        subreddit: str,
        sort: SubredditSort = SubredditSort.HOT,
        t: SubredditT = SubredditT.DAY,
        before: str | None = None,
        after: str | None = None
    ) -> ScrappitDispatcherTask:
        return self.put_task(ScrappitDispatcherTask("r", (subreddit, sort, t, before, after), priority=priority))

    def user_about(self, priority: int, username: str) -> ScrappitDispatcherTask:
        return self.put_task(ScrappitDispatcherTask("user_about", (username,), priority=priority))

    def user(
        self,
        priority: int,
        username: str,
        where: UserWhere = UserWhere.OVERVIEW,
        sort: UserSort = UserSort.NEW,
        t: UserT = UserT.ALL,
        before: str | None = None,
        after: str | None = None
    ) -> ScrappitDispatcherTask:
        return self.put_task(ScrappitDispatcherTask("user", (username, where, sort, t, before, after), priority=priority))

    def comments(
        self, priority: int, article: str, sort: CommentsSort = CommentsSort.CONFIDENCE, comment: str | None = None
    ) -> ScrappitDispatcherTask:
        return self.put_task(ScrappitDispatcherTask("comments", (article, sort, comment), priority=priority))

    def api_morechildren(
        self, priority: int, link_id: str, children: list[str], sort: CommentsSort = CommentsSort.CONFIDENCE
    ) -> ScrappitDispatcherTask:
        return self.put_task(ScrappitDispatcherTask("api_morechildren", (link_id, children, sort), priority=priority))
