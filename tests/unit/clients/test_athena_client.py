import pytest

from pyathena.pandas.cursor import PandasCursor

from tentaclio.clients.athena_client import AthenaClient


class TestAthenaClient:
    @pytest.mark.parametrize("url", ["postgresql://:@localhost", "s3://:@s3"])
    def test_invalid_scheme(self, url):
        with pytest.raises(ValueError):
            AthenaClient(url)

    @pytest.mark.parametrize(
        "url, username, password, hostname, port, database, query",
        [
            ("awsathena+rest://:@localhost", "", "", "localhost", None, "", None),
            (
                "awsathena+rest://:@localhost:5432/database?s3_staging_dir=dir",
                "",
                "",
                "localhost",
                5432,
                "database",
                {"s3_staging_dir": "dir"},
            ),
        ],
    )
    def test_parsing_athena_url(self, url, username, password, hostname, port, database, query):
        client = AthenaClient(url)

        assert client.drivername == "awsathena+rest"
        assert client.host == hostname
        assert client.username == username
        assert client.password == password
        assert client.port == port
        assert client.database == database
        assert client.url_query == query

    def test_get_df(self, mocker):
        url = "awsathena+rest://:@localhost"
        query = "SELECT * FROM bla"
        params = None

        mock_raw_cursor = mocker.Mock()
        mock_raw_cursor.execute = mocker.Mock()

        mock_raw_conn = mocker.Mock()
        mock_raw_conn.cursor = mocker.Mock(return_value=mock_raw_cursor)

        mocker.patch.object(AthenaClient, '_get_raw_conn', return_value=mock_raw_conn)

        client = AthenaClient(url)
        client.closed = False
        _ = client.get_df(sql_query=query)

        client._get_raw_conn.assert_called_once_with()
        mock_raw_conn.cursor.assert_called_once_with(PandasCursor)
        mock_raw_cursor.execute.assert_called_once_with(query, parameters=params)
