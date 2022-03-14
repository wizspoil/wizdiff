# install environment and packages with poetry
install:
  poetry install

# update deps
update:
  poetry update

# publish a major, minor or patch version
publish TYPE:
  poetry version {{TYPE}}
  poetry build
  poetry publish

