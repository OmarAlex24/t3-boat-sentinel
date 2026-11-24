FROM oven/bun:1-slim

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends curl ca-certificates unzip && \
    rm -rf /var/lib/apt/lists/*

# Install official Boat CLI into /root/.ascii/bin/boat
RUN curl -fsSL https://boat.dev/install.sh | sh
ENV PATH="/root/.ascii/bin:${PATH}"

WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production

COPY tsconfig.json ./
COPY src ./src
COPY scripts ./scripts

ENV SENTINEL_CONFIG=/config/sentinel.yaml
EXPOSE 8080

CMD ["bun", "run", "src/index.ts", "serve"]
