# Real-Time Fraud Detection

Сервис скоринга фродовых транзакций на потоке из Kafka. Модель CatBoost считает скор и флаг фрода, результат пишется в топик `scores`, оттуда отдельный сервис складывает его в Postgres, а в Streamlit-интерфейсе можно посмотреть последние фроды и гистограмму скоров.

Данные: https://www.kaggle.com/competitions/teta-ml-1-2025 (формат входных сообщений как в `test.csv`).

## Из чего состоит

Поток данных: interface -> топик `transactions` -> fraud_detector -> топик `scores` -> db_writer -> Postgres. Интерфейс читает результаты из Postgres напрямую.

- `interface` - Streamlit (порт 8501). Загружает CSV, каждую строку отправляет в `transactions` отдельным JSON с новым `transaction_id`. Внизу страницы есть кнопка «Посмотреть результаты»: она показывает 10 последних транзакций с `fraud_flag = 1` и гистограмму скоров последних 100 транзакций (если в базе их меньше, то по всем, что есть).
- `fraud_detector` - читает `transactions`, делает препроцессинг (`preprocessing.py`), считает скор моделью `my_catboost.cbm` (`scorer.py`), пишет в `scores`. Порог фрода 0.98. Только inference, только CPU.
- `db_writer` - читает `scores` и записывает `transaction_id`, `score`, `fraud_flag` в таблицу `scores` в Postgres. Если такой `transaction_id` уже есть, запись пропускается.
- `postgres` - база `fraud`, таблица создаётся из `db/init.sql`, данные лежат в volume `pgdata`.
- Kafka: zookeeper, broker, `kafka-setup` (создаёт топики `transactions` и `scores`, по 3 партиции) и Kafka UI на порту 8080.

Все контейнеры в одной сети `ml-scorer`.

## Запуск

Нужны Docker и Docker Compose. Должны быть свободны порты 8501, 8080, 9095, 5432 и 2181.

Модель `my_catboost.cbm` должна лежать в `fraud_detector/models/`, данные для препроцессинга - в `fraud_detector/train_data/`.

```bash
git clone <ссылка на репозиторий>
cd <папка репозитория>
docker-compose up --build
```

Первая сборка занимает несколько минут. Когда всё поднимется:

- интерфейс: http://localhost:8501
- Kafka UI: http://localhost:8080
- Postgres: localhost:5432, база `fraud`, пользователь `fraud`, пароль `fraud`

Остановить: `docker-compose down` (данные в базе останутся) или `docker-compose down -v` (вместе с данными).

## Как проверить, что всё работает

1. Откройте http://localhost:8501, загрузите CSV формата `test.csv` и нажмите «Отправить». Для проверки хватит 50-100 строк.
2. Подождите несколько секунд и нажмите «Посмотреть результаты» внизу страницы. Должны появиться таблица с фродовыми транзакциями (если фродов в выборке нет, будет надпись об этом) и гистограмма.
3. Сообщения в топиках `transactions` и `scores` видно в Kafka UI.
4. Содержимое базы можно посмотреть так:

```bash
docker-compose exec postgres psql -U fraud -d fraud -c "SELECT count(*) FROM scores;"
docker-compose exec postgres psql -U fraud -d fraud -c "SELECT * FROM scores WHERE fraud_flag = 1 ORDER BY created_at DESC LIMIT 10;"
```

Логи сервисов: `docker-compose logs fraud_detector`, `docker-compose logs db_writer`. Контейнер `kafka-setup` после создания топиков завершается сам, это нормально.

Если что-то пошло не так, самый простой вариант - `docker-compose down -v` и запуск заново. Kafka и Postgres стартуют не сразу, `fraud_detector` и `db_writer` дожидаются их сами, так что первые полторы-две минуты в логах могут быть сообщения об ожидании.

## Форматы сообщений

Вход (топик `transactions`):

```json
{
  "transaction_id": "d6b0f7a0-8e1a-4a3c-9b2d-5c8f9d1e2f3a",
  "data": {"transaction_time": "2023-01-01 12:30:00", "amount": 150.5, "...": "..."}
}
```

Выход (топик `scores`):

```json
{
  "score": 0.995,
  "fraud_flag": 1,
  "transaction_id": "d6b0f7a0-8e1a-4a3c-9b2d-5c8f9d1e2f3a"
}
```

Таблица `scores` в Postgres: `transaction_id` (первичный ключ), `score`, `fraud_flag` и `created_at` - время записи в базу, по нему выбираются последние транзакции.

## Структура репозитория

```
db/init.sql               создание таблицы scores
db_writer/                сервис записи из Kafka в Postgres
fraud_detector/           препроцессинг, модель, Kafka consumer/producer
interface/                Streamlit UI
docker-compose.yaml
README.md
```

Переменные окружения (адреса Kafka, названия топиков, параметры подключения к Postgres) заданы в `docker-compose.yaml`.
