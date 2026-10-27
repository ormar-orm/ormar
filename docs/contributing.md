All contributions to *ormar* are welcomed!

## Issues

To make it as simple as possible for us to help you, please include the following:

*  OS 
*  python version
*  ormar version
*  database backend (mysql, sqlite or postgresql)

Please try to always include the above unless you're unable to install *ormar* or **know** it's not relevant
to your question or feature request.

## Pull Requests

It should be quite straight forward to get started and create a Pull Request.

!!! note
    Unless your change is trivial (typo, docs tweak etc.), please create an issue to discuss the change before
    creating a pull request.

To make contributing as easy and fast as possible, you'll want to run tests and linting locally. 

You'll need to have **python 3.11** or newer (up to **3.15**), **poetry**, and **git** installed.

```bash
# 1. clone your fork and cd into the repo directory
git clone git@github.com:<your username>/ormar.git
cd ormar

# 2. Install ormar, dependencies and test dependencies
poetry install -E dev

# 3. Checkout a new branch and make your changes
git checkout -b my-new-feature-branch
# make your changes...

# 4. Formatting and linting
# ormar uses ruff for formatting and linting and mypy for type hints check
# run all of the following as all those calls will be run on travis after every push
ruff format ormar tests
ruff check ormar tests
flake8 ormar
mypy ormar tests

# 5. Run tests
# on localhost all tests are run against sglite backend
# rest of the backends will be checked after push
pytest -svv --cov=ormar --cov=tests --cov-fail-under=100 --cov-report=term-missing

# 6. Build documentation
mkdocs build
# if you have changed the documentation make sure it builds successfully
# you can also use `mkdocs serve` to serve the documentation at localhost:8000

# ... commit, push, and create your pull request
```

!!!tip
    For more information on how and why ormar works the way it works 
    please see the [API documentation][API documentation]

[API documentation]: ./api/index.md