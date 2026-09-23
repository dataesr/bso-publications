CURRENT_VERSION=$(shell cat bso/__init__.py | cut -d "'" -f 2)
DOCKER_IMAGE_NAME=dataesr/bso-publications
GHCR_IMAGE_NAME=ghcr.io/$(DOCKER_IMAGE_NAME)

test: unit

unit:
	@echo Running unit tests...
	python3 -m pytest
	@echo End of unit tests

install:
	@echo Installing dependencies...
	pip install -r requirements.txt
	@echo End of dependencies installation

docker-build-bso:
	@echo Building a new docker image for BSO
	docker build -t $(GHCR_IMAGE_NAME):$(CURRENT_VERSION) -t $(GHCR_IMAGE_NAME):latest .
	@echo Docker image built for BSO

docker-build-scanr:
	@echo Building a new docker image for scanR
	echo "__version__ = '$(CURRENT_VERSION)-scanr'" > bso/__init__.py
	docker build -t $(GHCR_IMAGE_NAME):$(CURRENT_VERSION)-scanr -t $(GHCR_IMAGE_NAME):latest .
	echo "__version__ = '$(CURRENT_VERSION)'" > bso/__init__.py
	@echo Docker image built for scanR

docker-push-bso:
	@echo Pushing a new docker image for BSO
	docker push -a $(GHCR_IMAGE_NAME)
	@echo Docker image pushed for BSO

docker-push-scanr:
	@echo Pushing a new docker image for scanR
	echo "__version__ = '$(CURRENT_VERSION)-scanr'" > bso/__init__.py
	docker push -a $(GHCR_IMAGE_NAME)
	echo "__version__ = '$(CURRENT_VERSION)'" > bso/__init__.py
	@echo Docker image pushed for scanR

release-bso:
	echo "__version__ = '$(VERSION)'" > bso/__init__.py
	git commit -am '[release] version $(VERSION)'
	git tag $(VERSION)
	@echo If everything is OK, you can push with tags i.e. git push origin main --tags

release-scanr:
	@echo Building a new docker image for scanR
	echo "__version__ = '$(CURRENT_VERSION)-scanr'" > bso/__init__.py
	docker build -t $(GHCR_IMAGE_NAME):$(CURRENT_VERSION)-scanr -t $(GHCR_IMAGE_NAME):latest .
	docker push -a $(GHCR_IMAGE_NAME)
	echo "__version__ = '$(CURRENT_VERSION)'" > bso/__init__.py
	@echo Docker image pushed for scanR
