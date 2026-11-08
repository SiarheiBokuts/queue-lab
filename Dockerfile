# Stage `web`: build the Vue frontend
FROM node:24-slim AS web
WORKDIR /web
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./
RUN npx vite build

# Stage `web-test`: type-check and unit-test the frontend.   docker build --target web-test .
FROM web AS web-test
RUN npm run typecheck && npm test

# Stage `backend-test`: lint and unit-test the backend.      docker build --target backend-test .
FROM python:3.14-slim-trixie AS backend-test
WORKDIR /app
COPY backend/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt pytest==9.2.0 ruff==0.17.4
COPY backend/pyproject.toml ./
COPY backend/app ./app
COPY backend/tests ./tests
COPY workers ./workers
COPY frontend/package.json ./frontend/package.json
COPY CHANGELOG.md ./CHANGELOG.md
RUN ruff check app tests && python -m pytest -q

# Final stage (the default): FastAPI backend that also serves the built frontend and carries the worker sources
FROM python:3.14-slim-trixie
WORKDIR /app
COPY backend/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY backend/app ./app
COPY workers ./workers
COPY --from=web /web/dist ./static
ENV PYTHONUNBUFFERED=1 DATA_DIR=/data
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
