[![PyPI - License](https://img.shields.io/pypi/l/databricks-sdk)](https://github.com/databricks/databricks-sdk-py/blob/main/LICENSE)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)


# About harv
**harv** is a collection of common functionality that support jobs and notebooks in databricks workspace.

<br>

# Getting Started

Follow these steps to get the project up and running on your local system.

## Installation Process

1. **Clone the repository**:
    ```sh
    git clone git@github.com:cltj/harv.git
    cd harv
    ```

2. **Install dependencies**:
    Use [uv](https://docs.astral.sh/uv/) as the package manager to install the required dependencies:
    ```sh
    uv install
    ```

3.	**Latest Releases**
    You can find the latest releases and release notes on the [CHANGELOG](https://github.com/cltj/harv?path=/CHANGELOG.md) page.

<br>
<br>
<br>


# Contribute
Respect the linting rules,

Follow the Coding Standards: Adhere to the coding standards and guidelines specified in the project. This includes following naming conventions, code formatting, and commenting practices.

Respect the Linting Rules: Use the specified linter (e.g., ruff) to ensure your code meets the project's linting rules. Fix any linting issues before submitting your code. Use the # TODO functionality for tasks, when in need or in a rush use the # noqa codes to produce.

Write Clear Commit Messages: Write descriptive and concise commit messages that explain the purpose of the changes. This helps in understanding the history of the project.

Create Meaningful Pull Requests: When creating a pull request, provide a clear description of the changes and the problem they solve. Reference any relevant issues or tasks.

Write Tests: Ensure that your code is well-tested. Write unit tests for new features and bug fixes. Make sure all tests pass before submitting your code.

Document Your Code: Update the documentation to reflect any changes or new features. This includes updating the README, API references, and any other relevant documentation.

Collaborate and Communicate: Communicate with other team members, ask for feedback, and collaborate effectively. Use comments and discussions to clarify any doubts or issues.


<br>
<br>
<br>

# Build and Test

## Build
To build the project, you need to have the necessary dependencies installed. Follow these steps:

1. **Build the project**:
    uv is configures to use setuptools behind the scenes as the backend build tool, so you can just run:
    ```sh
    uv build --package harv --wheel
    ```

## Test
To run tests, follow these steps:

1. **Lint the code**:
    Ensure your code meets the project's linting rules using [ruff](https://docs.astral.sh/ruff/):
    ```sh
    ruff check .
    ```



