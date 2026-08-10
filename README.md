# Log Ingestion and Query Service

خدمة Node.js/TypeScript لتخزين واستعلام structured logs. PostgreSQL هو مصدر الحقيقة الوحيد للقراءة والكتابة، وتبدأ الخدمة بالكامل عبر Docker Compose.

## التشغيل

```bash
docker compose up
```

عند اكتمال startup تصبح الخدمة متاحة على `http://localhost:8080`. لا تحتاج إلى ملف `.env` مع Docker Compose. استخدم `docker compose up --build` فقط إذا أردت فرض إعادة بناء الصورة بعد تعديل الكود. للاستخدام المحلي انسخ `.env.example` إلى `.env` ثم شغّل `npm run dev`. يستخدم Compose شبكة bridge مخصصة، فيتصل التطبيق بقاعدة البيانات عبر اسم الخدمة `postgres`، ويطبق حدود التقييم: التطبيق `0.5 CPU` و`256 MB`، وPostgreSQL `1 CPU` و`1 GB`.

## الواجهات

### `GET /health`

يعيد `200` فقط بعد الاتصال بقاعدة البيانات وتطبيق الـschema والفهارس. لا يتطلب مصادقة.

### `POST /logs`

يستقبل batch دائمًا، ولو احتوى على سجل واحد:

```json
{
  "logs": [{
    "timestamp": "2026-07-20T14:32:01.123Z",
    "level": "error",
    "service": "checkout",
    "message": "payment declined",
    "attributes": { "user_id": "42", "region": "eu-west", "retries": 3 }
  }]
}
```

يتحقق من كل عنصر بصورة مستقلة. القيم المسموحة لـ`level`: `debug`, `info`, `warn`, `error`. يجب أن يكون `timestamp` ISO 8601 ولا يتجاوز خمس دقائق في المستقبل؛ و`service` و`message` نصين غير فارغين؛ و`attributes` كائنًا مسطحًا بقيم string/number/boolean. إذا قُبل سجل واحد على الأقل يعيد `200`:

```json
{ "accepted": 1, "rejected": [] }
```

إذا رُفضت كل السجلات، أو كان body غير مطابق للعقد/JSON معطوبًا، يعيد `400`.

### `GET /logs`

يدعم الجمع الحر للـfilters: `service`, `level`, `since`, `until`, `q`, و`attr.<key>`. مثال:

```text
/logs?service=checkout&level=error&since=2026-07-20T00:00:00Z&attr.region=eu-west&q=declined&limit=100
```

قيمة `attr.<key>` تصل من query string، لذلك يطابق `attr.user_id=42` القيمة النصية `"42"` والقيمة العددية `42` على حد سواء؛ لا تضيع السمات النصية التي تبدو كأرقام أو booleans.

`limit` بين 1 و1000 (100 افتراضيًا). الترتيب `timestamp DESC, id DESC`. يعيد:

```json
{ "logs": [], "next_cursor": null }
```

عندما توجد صفحة أخرى، `next_cursor` قيمة opaque مشفرة؛ أرسلها كما هي في `cursor` للصفحة التالية. المعاملات غير الصالحة تعيد `{ "error": "..." }` مع `400`.

### `GET /logs/aggregate`

يدعم filters: `service`, `level`, `q` و`attr.<key>`، مع معاملات aggregation التالية:

- `since`: إلزامي، بداية المدى الزمنية inclusive بصيغة ISO 8601.
- `until`: إلزامي، نهاية المدى الزمنية exclusive بصيغة ISO 8601.
- `bucket`: إلزامي؛ إحدى القيم `1m` أو`5m` أو `1h` أو `1d`.
- `group_by`: اختياري؛ `service` أو `level` فقط.

مثال: `/logs/aggregate?since=2026-07-20T14:00:00Z&until=2026-07-20T15:00:00Z&bucket=5m&group_by=service&level=error`.

يعيد buckets صاعدة زمنيًا؛ عند عدم تقديم `group_by` تكون `group` هي `null`.

## تصميم قاعدة البيانات والفهارس

جدول `logs` يستخدم UUID، `timestamptz`، level/service/message النصية، و`jsonb` للـattributes. اختيار JSONB يسمح بتخزين حقول metadata الحرة دون جدول أعمدة متغير، مع بقاء PostgreSQL مصدر الحقيقة.

- `(timestamp DESC, id DESC)`: paging وترتيب ثابت.
- `(service, timestamp DESC, id DESC)` و`(level, timestamp DESC, id DESC)`: الاستعلامات ذات filter شائع.
- `GIN (attributes jsonb_path_ops)`: filters من `attr.<key>`.
- `GIN (message gin_trgm_ops)`: البحث الجزئي `q`.
- `log_second_rollups`: ملخصات transactionally-maintained حسب الثانية/service/level. يستخدمها aggregation عندما لا يوجد `q` أو `attr.*`، ويقرأ الصفوف الخام للحواف الزمنية غير المكتملة وللفلاتر التي لا يمكن تلخيصها؛ لذلك تبقى النتائج دقيقة وPostgreSQL مصدر الحقيقة.

تُطبّق هجرة Drizzle الموجودة في `drizzle/0000_initial.sql` عند startup، ولا تُعدّ الخدمة healthy قبل نجاحها. يستخدم ingestion بروتوكول PostgreSQL الأصلي `COPY FROM STDIN` عبر `postgres.js` بدل آلاف معاملات `INSERT`؛ لذلك تُرسل بيانات الـbatch في stream واحد مع backpressure، وتبقى عملية الـCOPY ذرّية: إمّا تُحفظ كل سجلات الـbatch أو لا يُحفظ أي منها.

## Retention

`RETENTION_DAYS` افتراضيها 30. عند البدء ثم كل ساعة، تحذف الخدمة السجلات الأقدم من هذا الحد، وتحدّث ملخصات التجميع في المعاملة نفسها. يمكن ضبط interval عبر `RETENTION_INTERVAL_MS`. لا توجد مصادقة أو rate limiting؛ كلاهما optional وغير مفعّل، كي لا يغيّر عقد الـload generator.

## التحقق والاختبارات

```bash
npm run typecheck
npm test
npm run smoke:test
TOTAL_LOGS=1000000 BATCH_SIZE=1000 CONCURRENCY=8 npm run load:test
```

تغطي اختبارات الوحدة validation للـbatch وcursor parser والتواريخ غير الموجودة. أما `smoke:test` فيتحقق من المسارات المطلوبة وسلوك batch partial rejection والـcursor وفلترة attributes النصية وaggregation الإلزامي بـ`5m`. يولّد `load:test` batches متوازية ويشغّل aggregation مرة كل ثانية، ثم يطبع معدل الإدخال وp50/p95. افتراضيًا تستخدم السكربتات `127.0.0.1:8080` لتجنب اختلاف localhost/IPv6 على Windows، ويمكن تغييرها عبر `BASE_URL`.

### نتائج قياس فعلية

تم القياس على Windows مع Docker Desktop، في قاعدة PostgreSQL نظيفة ومعزولة، وبحدود Compose: التطبيق `0.5 CPU` و`256 MB`، وPostgreSQL `1 CPU` و`1 GB`.

| المقياس | النتيجة |
| --- | ---: |
| حجم البيانات | 1,000,000 سجل |
| حجم الـbatch | 1,000 سجل |
| التوازي | 8 طلبات |
| السجلات المقبولة | 1,000,000 (دون رفض) |
| معدل الإدخال | 18,665.16 سجل/ثانية |
| Ingestion p50 | 396.27 ms للـbatch |
| Ingestion p95 | 775.17 ms للـbatch |
| Aggregation p50 | 97.36 ms |
| Aggregation p95 | 699.84 ms |
| ذروة التطبيق المُراقبة | 39.56% CPU، 84.89 MiB RAM |
| ذروة PostgreSQL المُراقبة | 105.01% CPU، 693.70 MiB RAM |

تحقق القياس من هدف الإدخال الأدنى (`15,000 log/sec`) ومن هدف p95 للتجميع (أقل من ثانية). سُجلت قمم الموارد عبر `docker stats` أثناء حمل متوازٍ؛ بقيت الذاكرة ضمن الحدود المفروضة للحاويتين.

## ملاحظات الأداء والحدود

أبرز bottleneck مكتشف كان إدخال rows عبر `INSERT` متعدد القيم تحت حمل متوازٍ؛ تم استبداله بـ`COPY FROM STDIN`. كما أن تجميع الصفوف الخام أثناء الإدخال يضغط على PostgreSQL، لذلك أضيفت ملخصات حسب الثانية مع fallback دقيق للصفوف الخام عند الحواف والفلاتر النصية/السمات. الفهارس الموجودة تحافظ على سرعة الفلاتر والـcursor، لكنها تضيف كلفة كتابة طبيعية. الاستعلامات النصية `q` والفلاتر على attributes غير شائعة هي الأعلى كلفة، لذلك تستخدم الخدمة فهرس GIN للـtrigram وفهرس GIN للـJSONB، ويفرض aggregation مدىً زمنيًا إلزاميًا لتفادي مسح غير مقيد للجدول.

## الميزات الاختيارية وCI

لا توجد ميزات اختيارية مفعلة: لا authentication، ولا API keys، ولا multi-tenancy، ولا rate limiting، ولا quota. لذلك تتجاهل الخدمة أي `Authorization` header وتعمل الواجهات الأربع دون إعداد مسبق عبر `docker compose up`.

يتضمن المشروع `smoke:test` لعقد الـAPI و`load:test` لقياس الأداء. يشغّل GitHub Actions في `.github/workflows/ci.yml` فحص الأنواع واختبارات الوحدة، ثم يبني Docker Compose ويشغّل smoke test في وضعي unauthenticated وauthentication مع مفتاح مزروع. والـload test يُشغّل يدويًا لأنه اختبار مليون سجل مكلف زمنيًا.

### الميزات الاختيارية

كل الميزات التالية additive ولا تغيّر شكل أو نجاح الواجهات المطلوبة عند تشغيل `docker compose up` بلا إعدادات:

- **Authentication وmulti-tenancy**: معطّلة افتراضيًا عبر `AUTH_ENABLED=false`. عند `AUTH_ENABLED=true` و`LOADGEN_API_KEY=<key>` تُزرع المفتاح تلقائيًا بصلاحيات ingest/query ضمن tenant `loadgen`. يدعم `Authorization: Bearer <key>` و`X-API-Key`، بينما يبقى `/health` عامًا.
- **Rate limiting**: معطّل عبر `RATE_LIMIT_ENABLED=false`. فعّله مع `RATE_LIMIT_REQUESTS` (الافتراضي 1000 طلب/دقيقة)؛ المفتاح المزروع لمولد الحمل معفى منه.
- **Backpressure**: معطّل عبر `BACKPRESSURE_ENABLED=false`. عند تفعيله يحد `MAX_CONCURRENT_INGESTIONS` (الافتراضي 16) ويرد `503` و`Retry-After` بدل فقد السجلات.
- **Dead letters**: معطّلة عبر `DEAD_LETTER_ENABLED=false`. عند تفعيلها تُحفظ السجلات المرفوضة وأسبابها في جدول `dead_letters` من دون تغيير استجابة `POST /logs`.
- **Metrics وdashboard**: `/metrics` يعرض عدادات Prometheus عندما `METRICS_ENABLED=true` (افتراضيًا true)، و`/dashboard` يعرض لوحة تشغيل خفيفة.
- **Live tail**: `/logs/tail` هو SSE للسجلات المقبولة حديثًا، ويعطّل عبر `LIVE_TAIL_ENABLED=false`.
- **Alert webhook**: معطّل عبر `ALERTS_ENABLED=false`. يتطلب `ALERT_WEBHOOK_URL` ويدفع حدثًا عند بلوغ `ALERT_ERROR_THRESHOLD` (الافتراضي 1) من سجلات error ضمن batch.
- **Custom query language**: استخدم المعامل الإضافي `query` مثل `query=service:checkout level:error attr.region:eu q:declined` في `/logs` أو `/logs/aggregate`؛ يبقى استعمال المعاملات القياسية كما هو.
- **Compression**: معطّل عبر `COMPRESSION_ENABLED=false`. عند تفعيله يضغط استجابات أكبر من 1KiB بـgzip فقط عندما يطلب العميل `Accept-Encoding: gzip`.
