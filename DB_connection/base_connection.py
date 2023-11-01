from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union, Tuple
import pandas as pd


class QueryResult(ABC):
    """
    Wraps the result of a query in BaseConnection.

    This ensures we don't couple our code to a specific query output format
    """

    def __init__(self, query_result: any):
        """
        :param query_result: The raw result from the db query
        """
        self._query_result = query_result

    @property
    @abstractmethod
    def _raw_result(self) -> any:
        """
        Public property to return the raw result of the query.

        :return: Raw query result that was passed to the QueryResult object
        """

    @abstractmethod
    def to_df(self, rename: Dict[str, str] = None, index: Union[str, List[str]] = None) -> pd.DataFrame:
        """
        Turns the query output into a DataFrame.
        With the columns inheriting their names from the db tables
        The rename operation will be performed before the set_index operation

        :param rename: Dict of column names to be renamed
        :param index: The columns we should set as the index
        :return: DataFrame result of the query
        """

    @abstractmethod
    def fetchone(self) -> Tuple[object]:
        """
        Fetches the first result/row in the query
        :return: first result/row in query
        """

    @abstractmethod
    def fetchall(self) -> List[Tuple[object]]:
        """
        Fetches all the rows of a query
        :return: all results/rows in a query
        """


class BaseConnection(ABC):
    """
    Base class to interact with a database or api that will return database-like responses
    """

    @abstractmethod
    def execute(self, to_execute: any) -> Optional[QueryResult]:
        """
        Executes a command to the connection
        ie if this is a SQL database this would execute a query, if it is an api this would make a request

        :return: QueryResult object containing the result of the query
            If the result of the query is None then will return None
        """

    @abstractmethod
    def close(self) -> None:
        """
        Closes the connection to the DB.

        Once this method is called the class is unusable again
        :return: None
        """


class DBBaseConnection(BaseConnection):
    @abstractmethod
    def df_to_sql(self, df: pd.DataFrame, table: Optional[str], append: bool) -> None:
        """
        Writes a pandas DataFrame to the database.
        
        There's probably more inputs needed here adjust the doc string upon implementation

        :param df: The DataFrame we are writing to the DB
        :param table: name of the table we are writing data to
            Must be given if writing to a relational database
        :param append: should the data be appended to the table or overwrite it
            Must be given if writing to a relational database
        :return: None
        """