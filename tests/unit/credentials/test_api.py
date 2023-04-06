from tentaclio import credentials, urls


def test_authenticate(mocker):
    injector = credentials.CredentialsInjector()
    injector.register_credentials(urls.URL("ftp://user:pass@google.com"))
    mock_cred = mocker.patch("tentaclio.credentials.api.load_credentials_injector")
    mock_cred.return_value = injector
    authenticated = credentials.authenticate("ftp://google.com/myfile.csv")
    assert authenticated.username == "user"
    assert authenticated.password == "pass"
