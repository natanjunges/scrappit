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
from typing import TypeAlias

JSON: TypeAlias = dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None


@dataclass
class ScrappitTask:
    task: str = field(compare=False)
    args: tuple = field(compare=False)
    kwargs: dict = field(default_factory=dict, compare=False)


@dataclass
class ScrappitResult:
    task: ScrappitTask
    value: JSON | Exception
