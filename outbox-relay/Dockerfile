# Unlike service-a, this build context is just this folder — relay.js
# doesn't import anything from proto/, so there's nothing outside
# outbox-relay/ it needs access to.

FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev

COPY . .

# Prometheus metrics endpoint. No other port needed — this process has
# no server of its own beyond that.
EXPOSE 9101

CMD ["node", "relay.js"]