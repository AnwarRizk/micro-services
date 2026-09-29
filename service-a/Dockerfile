# Build context is the PROJECT ROOT, not this folder — see docker-compose.yml
# (context: ., dockerfile: service-a/Dockerfile). This is required because
# server.js needs proto/adder.proto, which lives outside service-a/.

FROM node:20-alpine

WORKDIR /app

# Copy just the dependency manifest first and install, before copying any
# source code. Docker caches each instruction as a layer — as long as
# package*.json hasn't changed, this layer is reused on rebuilds instead of
# re-running npm install every time you edit server.js.
COPY service-a/package*.json ./service-a/
WORKDIR /app/service-a
RUN npm install --omit=dev

# Now copy the actual source: the proto contract (shared, lives at the repo
# root) and service-a's own code.
WORKDIR /app
COPY proto/ ./proto/
COPY service-a/ ./service-a/

WORKDIR /app/service-a

# 50051 = gRPC server, 9100 = Prometheus metrics endpoint
EXPOSE 50051 9100

CMD ["node", "server.js"]
