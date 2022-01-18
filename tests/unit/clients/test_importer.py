import pytest

from tentaclio.clients import importer


def package_lister_fake(packages):
    return lambda: packages


@pytest.mark.parametrize(
    "packages, expected_calls",
    [
        (["tentaclio_s3", "pandas", "numpy"], ["tentaclio_s3"]),
        (
            ["tentaclio_s3", "tentaclio_athena", "numpy"],
            ["tentaclio_s3", "tentaclio_athena"],
        ),
        (["pandas", "numpy"], []),
    ],
)
def test_importer(mocker, packages, expected_calls):
    patched_import = mocker.patch("tentaclio.clients.importer.importlib.import_module")
    importer.import_tentaclio_plugins(package_lister_fake(packages))
    assert patched_import.call_count == len(expected_calls)
    patched_import.assert_has_calls([mocker.call(call) for call in expected_calls])
