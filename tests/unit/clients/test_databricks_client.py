from tentaclio.clients.databricks_client import build_odbc_connection_string


def test_build_odbc_connection_string():
    output = build_odbc_connection_string(DRIVER="simba",
                                           PORT=443,
                                           UID="token")
    expected = "DRIVER%3Dsimba%3BPORT%3D443%3BUID%3Dtoken"
    assert output == expected
