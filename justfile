# install enviorment and packages with poetry
install:
  poetry install

# update deps
update:
  poetry update

# run wizdiff within it's enviorment
run:
  poetry run wizdiff

# publish a major, minor or patch version
publish TYPE:
  poetry version {{TYPE}}
  poetry build
  poetry publish

