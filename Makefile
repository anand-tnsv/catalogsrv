# Go parameters
GO := go
GOBUILD := $(GO) build
GOCLEAN := $(GO) clean
GOTEST := $(GO) test
GOGET := $(GO) get

# Main application directory
APPDIR := ./cmd

# Build directory
BUILDDIR := build

# Binary output name
APPNAME := hatchcatalogsrv

# Targets
.PHONY: all clean test

all: build

build: clean
	@echo "Building $(APPNAME)..."
	@mkdir -p $(BUILDDIR)
	$(GOBUILD) -o $(BUILDDIR)/$(APPNAME) $(APPDIR)

clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	@rm -rf $(BUILDDIR)

test:
	@echo "Running tests..."
	$(GOTEST) -count=1 ./...

