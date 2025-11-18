FROM python:3.14-slim-trixie
WORKDIR /app 
COPY requirements.txt . 
RUN pip install --no-cache-dir --prefer-binary -r requirements.txt 
COPY . . 
CMD ["python", "src/main.py"]
