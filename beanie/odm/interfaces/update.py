from abc import abstractmethod
from typing import Any, Dict, Mapping, Optional

from pymongo.client_session import ClientSession

from beanie.odm.bulk import BulkWriter
from beanie.odm.operators.update import CurrentDate, Inc, Set


class UpdateMethods:
    """Update methods"""

    @abstractmethod
    def update(
        self,
        *args: Mapping[str, Any],
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ):
        ...

    def set(
        self,
        expression: Dict[str, Any],
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ):
        """
        Set values

        Example:

        ```python

        class Sample(Document):
            one: int

        await Document.find(Sample.one == 1).set({Sample.one: 100})

        ```

        Uses [Set operator](https://roman-right.github.io/beanie/api/operators/update/#set)

        :param expression: Dict[str, Any] - keys and
        values to set
        :param session: Optional[ClientSession] - pymongo session
        :param bulk_writer: Optional[BulkWriter] - bulk writer
        :return: self
        """
        return self.update(
            Set(expression),
            session=session,
            bulk_writer=bulk_writer,
            **pymongo_kwargs,
        )

    def current_date(
        self,
        expression: Dict[str, Any],
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ):
        """
        Set current date

        Uses [CurrentDate operator](https://roman-right.github.io/beanie/api/operators/update/#currentdate)

        :param expression: Dict[str, Any]
        :param session: Optional[ClientSession] - pymongo session
        :param bulk_writer: Optional[BulkWriter] - bulk writer
        :return: self
        """
        return self.update(
            CurrentDate(expression),
            session=session,
            bulk_writer=bulk_writer,
            **pymongo_kwargs,
        )

    def inc(
        self,
        expression: Dict[str, Any],
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ):
        """
        Increment

        Example:

        ```python

        class Sample(Document):
            one: int

        await Document.find(Sample.one == 1).inc({Sample.one: 100})

        ```

        Uses [Inc operator](https://roman-right.github.io/beanie/api/operators/update/#inc)

        :param expression: Dict[str, Any]
        :param session: Optional[ClientSession] - pymongo session
        :param bulk_writer: Optional[BulkWriter] - bulk writer
        :return: self
        """
        return self.update(
            Inc(expression),
            session=session,
            bulk_writer=bulk_writer,
            **pymongo_kwargs,
        )
