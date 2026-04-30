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
ENV T3CODE_HOST=0.0.0.0
ENV T3CODE_PORT=3773
ENV T3CODE_NO_BROWSER=1

EXPOSE 3773

WORKDIR /app/apps/server
CMD ["bun", "run", "start"]
