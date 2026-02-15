docker network create woodpecker || true

docker rm -f woodpecker-server woodpecker-agent || true

docker run -d --name woodpecker-server --network woodpecker \
  -p 8000:8000 \
  -e WOODPECKER_OPEN=true \
  -e WOODPECKER_HOST=http://localhost:8000 \
  -v woodpecker-data:/var/lib/woodpecker \
  woodpeckerci/woodpecker-server@sha256:8272415a2c53569d2d6de4aa8a7f025a9aef07e724c97ad981d1bacffc6b33f5

docker run -d --name woodpecker-agent --network woodpecker \
  -e WOODPECKER_SERVER=woodpecker-server:9000 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  woodpeckerci/woodpecker-agent@sha256:9dab9f4aedbc058061a0ce1517c0718300b61d69b6bf19b4eec4d1441bdd3548
