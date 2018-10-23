.PHONY: build push run-docker run deploy

PROJECT = $(shell gcloud config get-value project)
NAME := scorecard
WD := $(dir $(realpath $(firstword $(MAKEFILE_LIST))))

build:
	docker build . -t $(NAME)

push: IMAGE = gcr.io/$(PROJECT)/$(NAME):$(shell docker images -q --no-trunc scorecard | sed -e 's,[^:]*:,,')
push: build
	echo $(IMAGE) > $(NAME)-image
	docker tag $(NAME) $(IMAGE)
	docker push $(IMAGE)

run-docker:
	docker run -i -p 5000:5000 -v $(WD)/secrets:/secrets -t $(NAME)

run:
	GITHUB_TOKEN_PATH=secrets/scorecard-github-access-token.txt python scorecard/scorecard.py

deploy:
	sed -e "s,@sha@,$(shell git rev-parse --short=12 HEAD)," \
	  -e "s,@image@,$(shell cat scorecard-image)," \
	  < deployment.yaml.in > deployment.yaml
	kubectl apply -f deployment.yaml
