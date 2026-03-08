.PHONY: all fmt mod-tidy build upx clean install gofumpt code

# Program name
PROGRAM := pts

# Dynamic variable
GIT_COMMIT_ID := $(git log --pretty=oneline -n 1 | awk '{print $1}')

# Output directory
OUTPUT_DIR := .
OUTPUT := $(OUTPUT_DIR)/$(PROGRAM)

# Build command variable
BUILD_CMD = GOOS=linux GOARCH=amd64 CGO_ENABLED=0 CC=musl-gcc go build \
	-ldflags '-s -w -extldflags "-static" \
	-X github.com/cd365/pts/app.GitCommitId=$(GIT_COMMIT_ID)' \
	-o $(OUTPUT) cmd/pts/main.go

all: fmt build

fmt:
	@for file in $$(find . -name "*.go"); do go fmt "$${file}"; done

mod-tidy:
	@go mod tidy

build:
	@$(BUILD_CMD)
	@file $(OUTPUT) || true
	@echo "build success"

upx:
	@upx -9 ${PROGRAM}

clean:
	@rm -f $(OUTPUT)

install:
	@go install mvdan.cc/gofumpt@latest

gofumpt:
	@gofumpt -w .

code: fmt gofumpt