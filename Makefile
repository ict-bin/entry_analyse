REGISTRY ?= 172.31.30.52:5000
IMAGE_REPO ?= ultramangaia/gaiasec-env
IMAGE_TAG := entry-analyse
LOCAL_IMAGE := $(REGISTRY)/$(IMAGE_REPO):$(IMAGE_TAG)
REMOTE_IMAGE := $(IMAGE_REPO):$(IMAGE_TAG)

all:
	@echo "Targets: "
	@make -qpRr | egrep -e '^[a-z].*:$$' | sed -e 's~:~~g' | grep -v 'all' | sort
pull:
	git checkout main
	git pull
commit:
	test -z "$$(git status --short)" || opencode run 'commit it'
build:
	docker build -t $(LOCAL_IMAGE) -f Dockerfile.chain .
	docker tag $(LOCAL_IMAGE) $(REMOTE_IMAGE)
push:
	test -z "$$(git cherry -v)" || opencode run 'push it'

push_image:
	docker push $(LOCAL_IMAGE)
push_image_remote:
	docker push $(REMOTE_IMAGE)
