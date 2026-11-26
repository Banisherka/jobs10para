# Асинхронное взаимодействие. Очереди

Сервис A принимает текст через HTTP, а сервис B переводит его в верхний регистр. Обработка занимает 2,5 секунды. Сервисы не вызывают друг друга напрямую: задачи и результаты передаются через RabbitMQ.

## Запуск

Запустите Docker Desktop или Docker Engine с Compose v2. Откройте терминал в папке с `docker-compose.yml` и выполните:

```sh
docker compose up
```

Дождитесь `Application startup complete` в логах `api` и `Listening on tasks` в логах `worker`. Запросы выполняйте во втором терминале. Документация API доступна на <http://localhost:8000/docs>.

## Проверка через curl

Следующий блок подходит для PowerShell. В Bash используйте `curl` вместо `curl.exe`, `sleep 3` вместо `Start-Sleep -Seconds 3` и присвойте переменную в виде `job_url='...'`.

```powershell
'{"text":"Hello, RabbitMQ!"}' | curl.exe -sS -i -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' --data-binary '@-'
```

Ожидаемый ответ, без служебных заголовков:

```http
HTTP/1.1 202 Accepted
location: /jobs/29afb8b1-4c65-4b78-b6c0-2eddfb4b9b69

{"id":"29afb8b1-4c65-4b78-b6c0-2eddfb4b9b69","status":"ACCEPTED"}
```

Подставьте значение `location` из своего ответа. Если сделать `GET` до завершения обработки, он вернёт `200 OK` со статусом `ACCEPTED`. Через три секунды при пустой очереди можно получить результат:

```powershell
$job_url = 'http://localhost:8000/jobs/29afb8b1-4c65-4b78-b6c0-2eddfb4b9b69'
Start-Sleep -Seconds 3
curl.exe -sS -i $job_url
```

```http
HTTP/1.1 200 OK

{"id":"29afb8b1-4c65-4b78-b6c0-2eddfb4b9b69","status":"DONE","result":"HELLO, RABBITMQ!","completedAt":"2026-11-25T12:34:56.789+00:00"}
```

Для неизвестного ID возвращается `404`. Если API не получил подтверждение публикации, он возвращает `503`. При обрыве связи до подтверждения публикации нельзя знать наверняка, принял ли брокер сообщение.

## Очереди и контракт

Откройте <http://localhost:15672/#/queues>. Логин `jobs`, пароль `jobs-demo`. На вкладке **Queues and Streams** в виртуальном хосте `/` видны `tasks` и `results`. У пустых очередей нулевые счётчики. `Ready` показывает сообщения, ожидающие доставки, а `Unacked` показывает доставленные сообщения без подтверждения обработки.

1. В `tasks` API публикует `JobRequested` с JSON `{"id":"<uuid>","text":"Hello, RabbitMQ!"}`.
2. В `results` обработчик публикует `JobCompleted` с JSON `{"id":"<uuid>","result":"HELLO, RABBITMQ!","completedAt":"2026-11-25T12:34:56.789+00:00"}`. Время передаётся в UTC. Имя события хранится в свойстве сообщения AMQP `type`, а `message_id` совпадает с ID задачи.
3. API читает `results` и меняет состояние задачи на `DONE` в памяти.

## Почему 202 и что происходит при сбое

`202 Accepted` означает, что задача принята, но ещё не обязательно выполнена. API ждёт только подтверждение публикации от RabbitMQ, а не результат от обработчика. Заголовок `Location` указывает, где проверить статус.

Обработчик отправляет ручной `ack` для `tasks` только после того, как RabbitMQ подтвердит запись `JobCompleted` в `results`. Если обработчик упадёт до `ack`, брокер после закрытия соединения вернёт неподтверждённую задачу в очередь. Это доставка **at least once** при сбое обработчика, а не **exactly once**. Очереди объявлены с `durable=True`, сообщения публикуются с `delivery_mode=2`, и у RabbitMQ есть том для данных. Один брокер не защищает от потери диска или тома.

Если обработчик упал после публикации, но до `ack`, результат может прийти повторно. Пока процесс жив, обработчик кэширует последние 1024 результата по паре `(id, text)` и повторно не вычисляет их. После перезапуска кэш пуст, но перевод текста в верхний регистр даёт тот же результат и не меняет внешние данные. API применяет только первый `JobCompleted` для данного ID и сохраняет исходный `completedAt`. После перезапуска API его состояние теряется, поэтому старые ID возвращают `404`, а события для неизвестных задач пропускаются.

## Демонстрация повторной доставки

На уже запущенном стенде включите режим сбоя только у обработчика:

```powershell
$env:DEMO_CRASH_BEFORE_ACK = '1'
docker compose up -d --no-deps --force-recreate worker
```

В Bash первая строка: `export DEMO_CRASH_BEFORE_ACK=1`.

Отправьте одну новую задачу тем же `POST`. Обработчик опубликует результат и завершится до `ack`. Compose снова запустит обработчик, и тот получит эту же задачу. Посмотрите события:

```sh
docker compose logs worker api
```

Для одного ID сначала появятся `redelivered=False` и `Demo crash after publishing and before ACK`, затем `redelivered=True` и ещё один `JobCompleted confirmed by broker`. В логах API будет `Ignoring duplicate JobCompleted`. Значения `result` и `completedAt` при повторном `GET` не меняются. Сбой включён только для первой доставки, поэтому та же задача не вызывает бесконечный перезапуск.

Верните обычный режим:

```powershell
Remove-Item Env:DEMO_CRASH_BEFORE_ACK
docker compose up -d --no-deps --force-recreate worker
```

В Bash первая строка: `unset DEMO_CRASH_BEFORE_ACK`. Для остановки всего стенда выполните `docker compose down`.

Подробнее: [подтверждения и повторная доставка в RabbitMQ 4.2](https://www.rabbitmq.com/docs/4.2/confirms).
