FROM python:3.12-slim
WORKDIR /app
RUN groupadd --gid 10001 picker && useradd --uid 10001 --gid picker --no-create-home picker \
    && mkdir /data && chown picker:picker /data
COPY product_picker /app/product_picker
USER 10001:10001
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
EXPOSE 8091
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s CMD python -c "import json,urllib.request; assert json.load(urllib.request.urlopen('http://127.0.0.1:8091/health', timeout=3))['status']=='ok'"
CMD ["python", "-m", "product_picker.server"]
