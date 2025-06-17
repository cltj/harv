import pytest
from harv.ingest.ingest import Ingest

def test_ingest_init_sets_attributes():
    ingest = Ingest(env="dev", usecase="my_usecase", storage_account_name="myorgingestadls", container_name="org-container-dataplatform-ingest")
    assert ingest.env == "dev"
    assert ingest.usecase == "my_usecase"
    assert ingest.storage_account_name == "myorgingestadls"
    assert ingest.container_name == "org-container-dataplatform-ingest"

def test_storage_path_is_correct():
    ingest = Ingest(env="qa", usecase="example", storage_account_name="myorgingestadls", container_name="org-container-dataplatform-ingest")
    expected_path = "abfss://org-container-dataplatform-ingest@myorgingestadlsqa.dfs.core.windows.net"
    assert ingest.storage_path == expected_path

@pytest.mark.parametrize("env", ["dev", "qa", "prod"])
def test_storage_path_varies_with_env(env):
    ingest = Ingest(env=env, usecase="dummy", storage_account_name="myorgingestadls", container_name="org-container-dataplatform-ingest")
    assert ingest.storage_path.endswith(f"@myorgingestadls{env}.dfs.core.windows.net")
