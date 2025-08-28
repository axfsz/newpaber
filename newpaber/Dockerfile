FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY news_bot_patched.py .
# 运行时挂载 .env 或用 compose 的 env_file
CMD ["python", "news_bot_patched.py"]

