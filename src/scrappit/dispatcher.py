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
from .api import RedditAPITask, RedditAPISubredditSort, RedditAPIT, RedditAPIUserWhere, RedditAPIUserSort, RedditAPICommentsSort, RedditAPI


@dataclass
class ScrappitDispatcherTask(ScrappitTask):
    priority: float = 0
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

    def get(self, endpoint: str, priority: float | None = None, **params: str) -> ScrappitDispatcherTask:
        if priority is None:
            priority = RedditAPITask.GET.value.priority

        return self.put_task(ScrappitDispatcherTask(RedditAPITask.GET.value.name, (endpoint,), params, priority))

    def listing(
        self, endpoint: str, before: str | None = None, after: str | None = None, priority: float | None = None, **params: str
    ) -> ScrappitDispatcherTask:
        if priority is None:
            priority = RedditAPITask.LISTING.value.priority

        return self.put_task(ScrappitDispatcherTask(RedditAPITask.LISTING.value.name, (endpoint, before, after), params, priority))

    def r_about(self, subreddit: str, priority: float | None = None) -> ScrappitDispatcherTask:
        if priority is None:
            priority = RedditAPITask.R_ABOUT.value.priority

        return self.put_task(ScrappitDispatcherTask(RedditAPITask.R_ABOUT.value.name, (subreddit,), priority=priority))

    def r(
        self,
        subreddit: str,
        sort: RedditAPISubredditSort = RedditAPISubredditSort.HOT,
        t: RedditAPIT = RedditAPIT.DAY,
        before: str | None = None,
        after: str | None = None,
        priority: float | None = None
    ) -> ScrappitDispatcherTask:
        if priority is None:
            priority = RedditAPITask.R.value.priority + sort.value.priority

            if sort in (RedditAPISubredditSort.TOP, RedditAPISubredditSort.CONTROVERSIAL):
                priority += t.value.priority
                priority /= 3
            else:
                priority /= 2

        return self.put_task(ScrappitDispatcherTask(RedditAPITask.R.value.name, (subreddit, sort, t, before, after), priority=priority))

    def user_about(self, username: str, priority: float | None = None) -> ScrappitDispatcherTask:
        if priority is None:
            priority = RedditAPITask.USER_ABOUT.value.priority

        return self.put_task(ScrappitDispatcherTask(RedditAPITask.USER_ABOUT.value.name, (username,), priority=priority))

    def user(
        self,
        username: str,
        where: RedditAPIUserWhere = RedditAPIUserWhere.OVERVIEW,
        sort: RedditAPIUserSort = RedditAPIUserSort.NEW,
        t: RedditAPIT = RedditAPIT.ALL,
        before: str | None = None,
        after: str | None = None,
        priority: float | None = None
    ) -> ScrappitDispatcherTask:
        if priority is None:
            priority = RedditAPITask.USER.value.priority + where.value.priority + sort.value.priority

            if sort in (RedditAPIUserSort.TOP, RedditAPIUserSort.CONTROVERSIAL):
                priority += t.value.priority
                priority /= 4
            else:
                priority /= 3

        return self.put_task(ScrappitDispatcherTask(RedditAPITask.USER.value.name, (username, where, sort, t, before, after), priority=priority))

    def comments(
        self, article: str, sort: RedditAPICommentsSort = RedditAPICommentsSort.CONFIDENCE, comment: str | None = None, priority: float | None = None
    ) -> ScrappitDispatcherTask:
        if priority is None:
            priority = RedditAPITask.COMMENTS.value.priority + sort.value.priority
            priority /= 2

        return self.put_task(ScrappitDispatcherTask(RedditAPITask.COMMENTS.value.name, (article, sort, comment), priority=priority))

    def api_morechildren(
        self, link_id: str, children: list[str], sort: RedditAPICommentsSort = RedditAPICommentsSort.CONFIDENCE, priority: float | None = None
    ) -> ScrappitDispatcherTask:
        if priority is None:
            priority = RedditAPITask.API_MORECHILDREN.value.priority + sort.value.priority
            priority /= 2

        return self.put_task(ScrappitDispatcherTask(RedditAPITask.API_MORECHILDREN.value.name, (link_id, children, sort), priority=priority))
