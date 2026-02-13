# =============================================================================
# Dockerfile for Tiki ETL Pipeline with Apache Airflow
# Vietnam E-commerce Analytics Platform
# =============================================================================
# Base: Apache Airflow 2.8.1
# Includes: Chrome (for Puppeteer), Node.js 20, Python data libs
# =============================================================================

FROM apache/airflow:2.8.1

# Switch to root for system package installation
USER root

# -----------------------------------------------------------------------------
# System Dependencies
# -----------------------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Build essentials
    build-essential \
    curl \
    wget \
    gnupg \
    ca-certificates \
    # Chrome dependencies
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libwayland-client0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils \
    # Additional libs
    libx11-xcb1 \
    libxcb1 \
    libxext6 \
    libxshmfence1 \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------------------------------------------------------
# Install Google Chrome Stable
# -----------------------------------------------------------------------------
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Verify Chrome installation
RUN google-chrome --version

# -----------------------------------------------------------------------------
# Install Node.js 20.x
# -----------------------------------------------------------------------------
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Verify Node.js installation
RUN node --version && npm --version

# -----------------------------------------------------------------------------
# Set Puppeteer to use system Chrome
# -----------------------------------------------------------------------------
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/google-chrome-stable

# -----------------------------------------------------------------------------
# Switch back to airflow user for Python packages
# -----------------------------------------------------------------------------
USER airflow

# -----------------------------------------------------------------------------
# Python Dependencies (with Airflow constraints)
# -----------------------------------------------------------------------------
# Use Airflow's constraint file to ensure compatible versions
ENV AIRFLOW_VERSION=2.8.1
ENV PYTHON_VERSION=3.8

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    -r /tmp/requirements.txt

# -----------------------------------------------------------------------------
# Environment Variables
# -----------------------------------------------------------------------------
ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/config/google_cloud_credentials.json

# -----------------------------------------------------------------------------
# Working Directory
# -----------------------------------------------------------------------------
WORKDIR /opt/airflow

# -----------------------------------------------------------------------------
# Health Check
# -----------------------------------------------------------------------------
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1
