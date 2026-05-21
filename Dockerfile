FROM oven/bun:1.3 as builder

WORKDIR /app

RUN apt-get update && apt-get install -y python3 make g++

COPY . .
RUN bun install
RUN bun run build

FROM oven/bun:1.3-slim

WORKDIR /app
COPY --from=builder /app /app

ENV NODE_ENV=production
ENV KDCODE_HOST=0.0.0.0
ENV KDCODE_PORT=3773
ENV KDCODE_NO_BROWSER=1

EXPOSE 3773

WORKDIR /app/apps/server
CMD ["bun", "run", "start"]
