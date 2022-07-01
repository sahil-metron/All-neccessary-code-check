# Wiz collector

## Collector base image creation

```
docker build --compress --force-rm --no-cache --file Dockerfile.base --tag devo.com/ifc_base:1.0 .
```

## Using Collector Server SDK

## Build and upload

Update `metadata.json` with proper version description and number (if try to upload any already existing version will fail):

```bash
collector-cli upload
```

### Save

Change `<version>` with a proper value.
```bash
docker save 837131528613.dkr.ecr.us-east-1.amazonaws.com/collectors/wiz:<version> | gzip > collector-wiz-docker-image-<version>.tgz
```

### Run

Go to proper Collector Server environment and follow the instruction described at [Devo documentation website](https://docs.devo.com):
* [Integration/demo](https://collector-server.data.devo.com/home)
* [Production](https://https://collector-server-prod.data.devo.com/)

## Using Docker

### Build - Manual procedure

Change `<version>` with a proper value.
```bash
docker build \
--compress \
--force-rm \
--no-cache \
--tag devo.com/collectors/wiz:<version> .
```

### Build - Using bash script

```bash
./build_tools/build_docker.sh
```

### Saving the Docker image as .tgz

Change `<version>` with a proper value.
```bash
docker save devo.com/collectors/wiz:<version> | gzip > collector-wiz-docker-image-<version>.tgz
```

### Load

```bash
gunzip -c collector-wiz-docker-image-<version>.tgz | docker load
```

### Running as a Docker container

Change `<version>` with a proper value.
```bash
docker run \
--name collector-wiz \
--volume $PWD/certs:/devo-collector/certs \
--volume $PWD/credentials:/devo-collector/credentials \
--volume $PWD/config:/devo-collector/config \
--volume $PWD/state:/devo-collector/state \
--env CONFIG_FILE=config.yaml \
--rm \
--interactive \
--tty \
devo.com/collectors/wiz:<version>
```

## Vulnerabilities check with Trivy

Change `<version>` with a proper value.
```bash
trivy image --severity CRITICAL,HIGH,UNKNOWN devo.com/collectors/wiz:<version>
```

### Docker

Change `<version>` with a proper value.
```bash
docker run aquasec/trivy image --severity CRITICAL,HIGH,UNKNOWN devo.com/collectors/wiz:<version>
```