# flink-playground-java-job

Flink playground job for k8s operator.

## How to build
In order to build the project one needs maven and java.
Please make sure to set Flink version in
* `pom.xml` file with `flink.version` parameter
* `Dockerfile` file with `FROM` parameter
* `playground.yaml` file with `flinkVersion` parameter
```
mvn clean install

# Build the docker image into minikube
eval $(minikube docker-env)
docker build -t playground:latest .
```

## How to prepare minikube
```
minikube ssh
mkdir -p /tmp/flink
chmod 777 /tmp/flink
```

## How to deploy
```
kubectl apply -f playground.yaml
```

## How to delete
```
kubectl delete -f playground.yaml
```
