from abc import abstractmethod
from typing import Any, Mapping, Optional

from pymongo.client_session import ClientSession

from beanie.odm.bulk import BulkWriter
from beanie.odm.operators import FieldName
from beanie.odm.operators.update import CurrentDate, Inc, Set


class UpdateMethods:
    """Update methods"""

    @abstractmethod
    def update(
        self,
        *expressions: Mapping[FieldName, Any],
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ):
        ...

    def set(
        self,
        expression: Mapping[FieldName, Any],
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

        Uses [Set operator](operators/update.md#set)

        :param expression: Dict[Union[ExpressionField, str], Any] - keys and
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
        expression: Mapping[FieldName, Any],
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ):
        """
        Set current date

        Uses [CurrentDate operator](operators/update.md#currentdate)

        :param expression: Dict[Union[ExpressionField, str], Any]
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
        expression: Mapping[FieldName, Any],
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

        Uses [Inc operator](operators/update.md#inc)

        :param expression: Dict[Union[ExpressionField, str], Any]
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
