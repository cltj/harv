class Ingest:
    """Class to handle ingestion of data into the dataplatform."""

    def __init__(
        self,
        env: str,
        usecase: str,
        storage_account_name: str,
        container_name: str,
    ) -> None:
        """
        Initialize the Ingest class with environment, use case, storage account, and container.

        Args:
            env (str): The environment to set (e.g., 'dev', 'qa', 'prod').
            usecase (str): The use case for the ingestion process.
            storage_account_name (str): The name of the storage account for the ingestion process.
            container_name (str): The name of the container in the storage account.

        """
        self.env = env
        self.usecase = usecase
        self.storage_account_name = storage_account_name
        self.container_name = container_name

    @property
    def storage_path(self) -> str:
        """
        Get the storage path for the ingestion process.

        Returns:
            str: The storage path for the environment's ingest account.

        """
        return f"abfss://{self.container_name}@{self.storage_account_name}{self.env}.dfs.core.windows.net"
