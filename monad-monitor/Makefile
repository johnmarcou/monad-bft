IMAGE_NAME ?= monad-monitor
IMAGE_TAG  ?= latest
CONTEXT    := ..
DOCKERFILE := Dockerfile

.PHONY: build run clean extract

build:
	docker build -f $(DOCKERFILE) -t $(IMAGE_NAME):$(IMAGE_TAG) $(CONTEXT)

run:
	docker run --rm -it $(IMAGE_NAME):$(IMAGE_TAG)

clean:
	docker rmi $(IMAGE_NAME):$(IMAGE_TAG)

# Writes the built binary to /tmp/monad-monitor on the Docker daemon's host
# (via docker-compose.yml's bind mount) — works against a remote context too.
extract:
	docker compose run --rm extract
