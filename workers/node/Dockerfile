# Worker image (Node.js, TypeScript). Built by the app through the Docker API the first time Node.js workers are used.
# Unit tests have their own file: docker build -f workers/node/Dockerfile.test workers/node

# Node.js 24 and not 26 like the frontend build: @confluentinc/kafka-javascript (1.11.0) has no prebuilt binary for Node 26 yet
# (and building it from source fails). Move to 26 when a release with one is out.

# Stage 1: compile TypeScript (also type-checks the code)
FROM node:24-slim AS build
WORKDIR /app
COPY package.json package-lock.json tsconfig.json tsconfig.build.json ./
RUN npm ci --no-audit --no-fund
COPY src ./src
RUN npm run build

# Stage 2: runtime image with production dependencies and the compiled code only
FROM node:24-slim
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund
COPY --from=build /app/dist ./dist
CMD ["node", "dist/main.js"]
