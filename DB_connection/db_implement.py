from abc import ABC, abstractmethod
from typing import Dict, Generator, List, Optional, Union

from base_connection import *
import psycopg as pg
from sqlalchemy import create_engine
import csv
from io import StringIO


class PostgresQR(QueryResult):
    """
    Postgres implementation of Query Result to handle output of SQL queries
    """
    @property
    def _raw_result(self) -> any:
        """
        returns raw cursor object in PostgreSQL
        :return: cursor containing result of Postgres Query
        """
        return self._query_result

    def to_df(self, rename: Dict[str, str] = None, index: Union[str, List[str]] = None) -> pd.DataFrame:
        """
        Turns the query output into a DataFrame.
        With the columns inheriting their names from the db tables
        The rename operation will be performed before the set_index operation

        :param rename: a dictionary mapping old column names to new column names
        :param index: a string or list of strings of column names to set as index
        :return: pd.Dataframe with column names from the rename dict if passed and the column that is used as an index
        is the column passed in the index argument
        """
        colnames = [desc[0] for desc in self._query_result.description]
        df = pd.DataFrame(self._query_result.fetchall(), columns=colnames)
        if rename is not None:
            df.rename(columns=rename, inplace=True)
        if index is not None:
            df.set_index(keys=index, inplace=True)
        return df

    def fetchone(self) -> Tuple[object]:
        """
        returns one row at a time from Postgres Query Result
        :return: tuple
        """
        return self._query_result.fetchone()

    def fetchall(self) -> List[Tuple[object]]:
        """
        returns all data from Postgres query result object as list of tuples
        :return: List of tuples
        """
        return self._query_result.fetchall()


class PostgresConn(DBBaseConnection):
    """
    implementation of DB Connection for Postgres
    """

    def __init__(self, db: str, user: str, pw: str, host: str, port: str = '5432'):
        """
        makes connection to PostgresSQL db and initializes cursor object
        :param db: name of database
        :param user: which user is authorized for the db
        :param pw: password for db connection
        :param host: string for host, e.x. localhost
        :param port: port where Postgres is on, default 5432
        """
        self.db = db
        self.user = user
        self.pw = pw
        self.host = host
        self.port = port
        self.conn = pg.connect(
            dbname=db,
            user=user,
            password=pw,
            host=host,
            port=port
        )
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

    def execute(self, to_execute: any) -> Optional[QueryResult]:
        """
        takes a sql statement
        and returns a QueryResult object with result of query
        :param to_execute: a string, sql statement
        :return: cursor returned from the executed query wrapped as instance of query result class
        """
        qr = self.cur.execute(to_execute)
        self.conn.commit()
        return PostgresQR(qr)

    def close(self):
        """
        closes connection to Postgres DB
        :return: None
        """
        self.cur.close()
        if self.conn is not None:
            self.conn.close()

    @staticmethod
    def _psql_insert_copy(table, conn, keys, data_iter):
        """
        Execute SQL statement inserting data
        table : pandas.io.sql.SQLTable
        conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
        keys : list of str
            Column names
        data_iter : Iterable that iterates the values to be inserted
        """
        # gets a DBAPI connection that can provide a cursor
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            s_buf = StringIO()
            writer = csv.writer(s_buf)
            writer.writerows(data_iter)
            s_buf.seek(0)

            columns = ', '.join(['"{}"'.format(k) for k in keys])
            if table.schema:
                table_name = '{}.{}'.format(table.schema, table.name)
            else:
                table_name = table.name

            sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(table_name, columns)
            cur.copy_expert(sql=sql, file=s_buf)

    def df_to_sql(self, df: pd.DataFrame, table: Optional[str], append: bool) -> None:
        """
        take pandas df and insert into PostgreSQL db
        :param df: input pandas dataframe to insert into sql
        :param table: string of table to insert into
        :param append: boolean on whether to append to or overwrite table if exists
        :return: None
        """
        connect_alchemy = f"postgresql+psycopg2://{self.user}:{self.pw}@{self.host}:{self.port}/{self.db}"
        engine = create_engine(connect_alchemy)
        append_param = 'append' if append else 'replace'
        df.to_sql(table, con=engine, if_exists=append_param, index=False, method=self._psql_insert_copy)
