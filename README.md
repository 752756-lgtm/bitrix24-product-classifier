# Bitrix24 Call Summarizer

Сервис принимает расшифровку входящего звонка, находит связанную сделку и автоматически:

1. делает короткое резюме разговора и добавляет его в таймлайн сделки;
2. заменяет название сделки на конкретную суть звонка;
3. если обсуждалась конкретная группа товаров, заполняет поля `Категория товаров (Сайт)` и `Подкатегория товаров (Сайт)`.

Категории загружаются из YML. Модель может выбрать только существующую в каталоге пару «категория → подкатегория». Если звонок не относится к определенной товарной группе, поля категории не изменяются.

## Поддерживаемые события

`POST /bitrix/call-transcript` принимает:

- JSON с `deal_id` и `transcript`;
- JSON/form-urlencoded с `activity_id` — сервис сам читает активность через `crm.activity.get`, берет `DESCRIPTION` и находит привязанную сделку;
- стандартные ключи события Битрикс24 `data[FIELDS][ID]`, `data[FIELDS][OWNER_ID]` и `data[FIELDS][DESCRIPTION]`.

Для старой сделки доступен `POST /bitrix/backfill-deal/{deal_id}`. Сервис получает все связанные дела типа «Звонок», начиная с последнего, и использует первую доступную готовую расшифровку через `crm.activity.call.getTranscript`.

Пример проверки без записи в CRM:

```bash
curl -X POST 'https://service.example.ru/bitrix/call-transcript?dry_run=true' \
  -H 'Content-Type: application/json' \
  -H 'X-Webhook-Secret: your-secret' \
  -d '{"deal_id":123,"transcript":"Клиенту нужен самоходный штабелер 1,5 тонны с высотой подъема 4,5 метра"}'
```

## Настройка

1. Скопируйте `.env.example` в `.env` и заполните значения.
2. Входящему вебхуку Битрикс24 выдайте права CRM на чтение активностей, чтение и изменение сделок, добавление комментариев таймлайна.
3. Укажите URL YML и ключ OpenAI API.
4. Настройте в Битрикс24 отправку события после появления расшифровки разговора на `/bitrix/call-transcript`.

Для стабильной работы рекомендуется указать постоянные коды пользовательских полей в `CATEGORY_FIELD_ID` и `SUBCATEGORY_FIELD_ID`. Если они не заданы, сервис ищет поля по названиям. Для полей-списков текстовые значения автоматически преобразуются в ID вариантов.

## Запуск через Docker

```bash
docker build -t bitrix24-call-summarizer .
docker run -d --restart unless-stopped --env-file .env -p 8080:8080 bitrix24-call-summarizer
```

Проверка:

```bash
curl http://localhost:8080/health
```

Проверка старой сделки без записи:

```bash
python -m classifier.backfill --deal-id 314319
```

Запись подтвержденного результата:

```bash
python -m classifier.backfill --deal-id 314319 --write
```

## Ручное развёртывание через GitHub Actions

Workflow `.github/workflows/deploy.yml` запускается только вручную. Перед заменой контейнера он:

1. проверяет конфигурацию;
2. запускает тесты;
3. собирает новый Docker-образ на сервере;
4. выполняет обязательный dry-run указанной сделки без записи в CRM;
5. запускает сервис и проверяет `/health`;
6. при неуспешной проверке пытается вернуть предыдущий образ.

Один раз добавьте в `Settings → Secrets and variables → Actions` следующие Repository secrets:

| Secret | Значение |
|---|---|
| `SERVER_HOST` | IP или домен сервера |
| `SERVER_USER` | SSH-пользователь |
| `SERVER_PASSWORD` | SSH-пароль; хранится только в GitHub Secrets |
| `BITRIX_WEBHOOK_URL` | базовый URL входящего вебхука Битрикс24 |
| `OPENAI_API_KEY` | ключ OpenAI API |
| `WEBHOOK_SECRET` | отдельная длинная случайная строка для защиты HTTP endpoint |

После попадания workflow в основную ветку откройте `Actions → Deploy call summarizer → Run workflow`, укажите ID сделки и запустите. По умолчанию используется `314319`. Workflow не содержит режима `--write` и не изменяет CRM.

Сервис публикуется только на `127.0.0.1:8080`. Для приема событий Битрикс24 нужен HTTPS reverse proxy; его следует включать отдельно после успешного dry-run.

## Переменные окружения

| Переменная | Назначение |
|---|---|
| `BITRIX_WEBHOOK_URL` | Базовый URL входящего вебхука Битрикс24 |
| `OPENAI_API_KEY` | Ключ OpenAI API |
| `OPENAI_MODEL` | Модель анализа, по умолчанию `gpt-5.6-luna` |
| `YML_URL` | URL YML-фида с деревом категорий |
| `CATEGORY_FIELD_NAME` | Название поля категории |
| `SUBCATEGORY_FIELD_NAME` | Название поля подкатегории |
| `CATEGORY_FIELD_ID` | Постоянный код поля категории (`UF_CRM_...`) |
| `SUBCATEGORY_FIELD_ID` | Постоянный код поля подкатегории (`UF_CRM_...`) |
| `WEBHOOK_SECRET` | Секрет, ожидаемый в заголовке `X-Webhook-Secret` |
| `HTTP_TIMEOUT` | Таймаут внешних запросов |
| `TITLE_MAX_LENGTH` | Максимальная длина названия сделки |

## Проверки

```bash
python -m unittest discover -s tests -v
```

Для надежного результата используется Structured Outputs в OpenAI Responses API: ответ всегда содержит заголовок, резюме, признак товарного звонка и пару категории/подкатегории по заданной JSON-схеме.
