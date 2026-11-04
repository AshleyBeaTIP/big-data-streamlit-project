# How to Run This Project

This guide provides step-by-step instructions for setting up and running the Binance Kafka-Spark-MongoDB dashboard project on another machine.

---

## Prerequisites

Before starting, ensure the following tools are installed on your system:

* **Docker Desktop** (running, with WSL 2 backend on Windows)
* **Git**
* **Miniconda** or **Anaconda** (Python 3.10)

---

## Step-by-Step Setup Guide

### 1. Clone the Repository and Open the Directory

```bash
git clone https://github.com/YOUR-USERNAME/YOUR-REPO.git
cd YOUR-REPO
code .
```

---

### 2. Set Up the Python Environment

Create and activate a new Conda environment, then install the required Python packages:

```bash
conda create -n bigdata python=3.10.13 -y
conda activate bigdata
pip install -r requirements.txt
```

> **Note:** If Conda displays a Terms of Service prompt, run the suggested `conda tos accept ...` commands and re-run the `conda create` command above.

---

### 3. Environment Configuration

Create a local `.env` file from the repository's template:

* **Windows (PowerShell):**
  ```powershell
  Copy-Item .env.example .env
  ```
* **macOS / Linux (Bash/Zsh):**
  ```bash
  cp .env.example .env
  ```

---

### 4. Start Docker Containers & Spark Pipeline

1. **Start Kafka and MongoDB services:**
   ```bash
   docker compose up -d kafka mongo
   ```
2. **Wait about 15 seconds** for the Kafka broker to initialize, then create the Kafka topic:
   ```bash
   docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic crypto_prices --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```
3. **Build and start the Spark container:**
   ```bash
   docker compose up -d --build spark
   ```
4. **Verify container status:**
   ```bash
   docker compose ps
   ```
   *Ensure all three containers (`kafka`, `mongo`, and `spark`) display a state of **Up**.*

---

### 5. Start the Producer

Open a **new terminal** window, activate your environment, and execute the data producer script:

```bash
conda activate bigdata
python producer.py
```

---

### 6. Launch the Dashboard Application

Open a **third terminal** window, activate your environment, and start the Streamlit web app:

```bash
conda activate bigdata
streamlit run app.py
```

Once running, access the dashboard in your web browser at:
[http://localhost:8501](http://localhost:8501)

---

## What to Expect & Troubleshooting

* **Initial Data History:** Historical records are stored in a local Docker volume on each machine and are not committed to Git. The database will start empty and populate as your producer ingests live data.
* **Live Charts Warm-up:** The live page charts and aggregation tables require approximately 1 minute of streaming data to begin displaying visual data.
* **Binance Region Restrictions:** If Binance API access is restricted or blocked in your region, the producer script may throw connection/API errors.
* **Dockerfile Base Image Verification:** Ensure `spark/Dockerfile` uses `FROM python:3.10-slim-bookworm` to ensure successful builds across all Docker versions.