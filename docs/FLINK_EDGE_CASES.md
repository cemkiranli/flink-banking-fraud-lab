# Flink görüşme notları ve edge-case kataloğu

## 1. Time semantiği

**Event time** olayın üretildiği zamandır; replay edildiğinde aynı sonucu verebilir. **Processing time** operator'ün olayı gördüğü host saatidir; düşük latency sağlar ama network gecikmesine göre sonucu değişir. Bankacılık raporu ve fraud penceresi çoğunlukla event time ister.

Watermark `t`, sistemin `t` zamanına kadar olan event'lerin büyük ölçüde geldiği iddiasıdır; kesin garanti değildir. Bir operator birden çok input/partition alıyorsa etkin watermark minimum input watermark'ıdır.

Edge-case'ler:

- Boş partition watermark'ı sonsuza kadar durdurabilir. Lab'da `withIdleness(5s)` bunu çözer.
- Çok agresif watermark çok sayıda late event üretir; çok gevşek watermark sonucu geciktirir ve state'i büyütür.
- Event timestamp saniye iken millis sanılırsa 1970 pencerelerine gider. Contract ve range validation eklenmelidir.
- Gelecekten gelen bozuk timestamp watermark'ı ileri sıçratıp normal event'leri late yapabilir. Production validator `eventTime <= now + clockSkew` kontrol etmelidir.
- Host clock drift processing-time timer, TTL ve latency metric'lerini bozar; NTP gerekir.
- `allowedLateness`, watermark bekleme süresine ek state-retention süresidir. Geç event geldiğinde window yeniden fire edebilir; downstream upsert/retraction semantiğine hazır olmalıdır.

## 2. State, key ve TTL

Keyed state yalnız `keyBy` sonrasında erişilebilir; her key'in state'i mantıksal olarak ayrıdır. Broadcast state her parallel subtask'ta rule setinin bir kopyasını tutar.

- Hot key (`accountId=unknown`) tek subtask'ı boğar. Key kalitesini ölç, gerekiyorsa iki aşamalı aggregation/salting uygula.
- Kafka partition sayısı source parallelism'den düşükse bazı source subtasks boş kalır.
- Parallelism değişince keyed state key-group'lar üzerinden redistribute edilir; `maxParallelism` gelecekteki ölçekleme sınırını etkiler.
- TTL yalnız processing time kullanır. 24 saat event-time dedup garantisi değildir; replay çok eskiyse duplicate tekrar kabul edilir.
- TTL temizliği best-effort'tur; expired kayıt okunamaz olsa bile fiziksel disk hemen küçülmeyebilir.
- TTL config checkpoint/savepoint'in parçası değildir. Restore sırasında TTL'yi kısadan uzuna değiştirmek semantik veri hatası yaratabilir.
- Çok uzun dedup TTL state patlaması; çok kısa TTL duplicate kaçırılması demektir. Kaynağın maksimum retry/replay süresiyle belirlenir.
- Broadcast rule update'leri tüm task'lara aynı sırayla uygulanmalıdır. Kafka rule topic key/partition politikası ve monoton `version` eski mesajın yeni kuralı ezmesini önler.
- Bu lab yalnız en yeni version'ı tutar. Geç event'e olay zamanındaki eski rule gerekiyorsa temporal/versioned rule state tasarlanmalıdır.

## 3. Delivery guarantee gerçeği

Exactly-once yalnız uçtan uca zincirin en zayıf halkası kadar güçlüdür:

1. Kafka source offset'i checkpoint state'ine girer.
2. Operator state aynı checkpoint barrier ile snapshot edilir.
3. Kafka sink transaction açar; checkpoint tamamlanınca commit eder.
4. Consumer `isolation.level=read_committed` kullanmazsa abort edilmemiş/uncommitted kayıtları görebilir.

Edge-case'ler:

- Checkpoint kapalıysa Kafka exactly-once sink çalışmaz.
- Kafka `transaction.timeout.ms`, en kötü checkpoint + restart süresinden kısa olursa broker transaction'ı abort eder / producer fence olabilir.
- İki job aynı transactional-id prefix'i kullanırsa birbirini fence edebilir. Prefix deployment başına unique olmalıdır.
- Harici REST çağrısını sıradan `map()` içinde yapmak exactly-once değildir. Idempotency key, async I/O + retry politikası veya transactional outbox gerekir.
- `System.out` sink'i exactly-once değildir. Logda duplicate görmek Flink state'inin yanlış olduğu anlamına gelmez.
- Checkpoint başarıya ulaşmadan oluşan output Kafka'da görünmeyebilir; exactly-once visibility latency'si checkpoint interval ile bağlantılıdır.
- Source retention checkpoint'in gösterdiği offset'i silerse restore mümkün olmayabilir.

## 4. Checkpoint, savepoint ve upgrade

Checkpoint otomatik fault recovery içindir; savepoint operatör kontrollü deployment/upgrade içindir. Externalized checkpoint'i cancellation sonrası tutmak onu otomatik olarak iyi bir release artifact'i yapmaz.

- Stable `uid` state'i yeni job graph operator'üne eşler. UID değişirse state restore edilemez veya `--allowNonRestoredState` ile kaybedilir.
- Serializer/model schema değişikliği state migration gerektirebilir. Alan eklemek her serializer için otomatik güvenli değildir.
- Operator chain/topology değişimi state mapping ve performance'ı etkiler.
- Unaligned checkpoint backpressure altında süreyi düşürebilir fakat storage/network maliyeti ve recovery davranışı farklıdır.
- Incremental checkpoint yalnız backend/storage kombinasyonu destekliyorsa fayda sağlar.
- Checkpoint tamamlanıyor olması sağlıklı throughput anlamına gelmez; duration, alignment time, size ve age birlikte izlenir.

## 5. Windows ve sonuç semantiği

Lab risk özeti için `[minuteStart, minuteEnd)` tumbling window; velocity için 60 saniyelik, 10 saniyede kayan sliding window kullanır.

- Sınırdaki event (`timestamp == windowEnd`) sonraki pencereye girer.
- Sliding window aynı event'i altı pencereye sokabilir; state/CPU maliyeti slide küçüldükçe artar.
- Velocity alert aynı olay kümesi için birden fazla örtüşen alert üretebilir. Downstream alert suppression/dedup business kararıdır.
- `AggregateFunction` incremental olduğu için full-window listesine göre state'i küçültür. `ProcessWindowFunction` yalnız window metadata eklemek için kombine edilir.
- Allowed late event summary'yi yeniden üretir. Kafka topic log olduğu için aynı key ile compacted topic veya consumer-side upsert tercih edilebilir.
- Side output'a düşen "too late" kayıt kaybolmuş sayılmamalı; audit, replay ve operasyon metriği olmalıdır.

## 6. Serialization ve veri contract'ı

- Flink POJO için public class, no-arg constructor ve erişilebilir getter/setter kullanıldı.
- Para minor unit `long`; `double` ile `0.1 + 0.2` hataları fraud boundary'sini bozar. Currency scale (JPY=0, KWD=3) gerçek sistemde ayrı metadata ister.
- JSON öğrenmek için kolaydır ama schema evolution zayıftır. Production'da Schema Registry ve compatibility modu gerekir.
- Unknown JSON field'lar forward compatibility için tolere ediliyor; eksik zorunlu alanlar validator ile DLQ'ya gidiyor.
- Poison pill job'ı sürekli restart loop'a sokmamalı. Transaction parse hatası DLQ; rule parse hatası ise bilinçli olarak job'ı fail eder çünkü yanlış rule ile devam etmek risklidir.

## 7. Backpressure ve performans

- Backpressure downstream yavaşlığının upstream'e yayılmasıdır. Flink UI'da busy/backpressured/idle oranlarını ve subtask skew'u karşılaştır.
- `keyBy` network shuffle oluşturur; serialization ve network buffer maliyeti vardır.
- Object reuse açılırsa mutable object'i state/output arasında tekrar kullanmak veri bozulmasına yol açabilir.
- Sink throughput'u checkpoint sırasında flush beklediğinde checkpoint uzar.
- Bir task slot kaynak izolasyonu değildir; aynı slot sharing group içindeki operator'ler JVM/resource paylaşır.
- Async enrichment'te capacity düşükse throughput düşer, yüksekse dış servis çöker. Timeout, ordered/unordered wait ve retry ayrı kararlar.

## 8. Bu projeye eklenecek ileri seviye alıştırmalar

1. Üç decline ardından success desenini Flink CEP ile, timeout side output ile kur.
2. Account profilini Postgres CDC ile alıp temporal join veya broadcast enrichment uygula.
3. JSON yerine Avro + Schema Registry geçir ve backward-compatible alan ekle.
4. Filesystem/hashmap yerine S3-compatible MinIO + RocksDB/ForSt backend dene.
5. Prometheus reporter ve Grafana dashboard ekle; watermark lag ve checkpoint alert'i yaz.
6. Savepoint al, `VelocityAlertWindowFunction` kodunu değiştir, UID koruyarak restore et.
7. Deliberate sink delay ekleyip backpressure ve aligned/unaligned checkpoint farkını ölç.
8. Rule history tutup late event'i `effectiveFrom` zamanındaki rule version ile değerlendir.

## 9. Görüşmede kısa cevap şablonları

**Kafka Streams yerine Flink?** Flink; birden fazla source/sink, gelişmiş event-time/window/CEP, cluster resource yönetimi ve büyük state için daha genel bir distributed engine'dir. Kafka Streams daha küçük operational footprint ile Kafka-merkezli uygulamada güçlüdür.

**Exactly-once ne demek?** Her fiziksel işlem yalnız bir kez çalışır demek değildir. Failure sonrası tekrar çalışabilir; fakat state ve destekleyen sink'in gözlemlenebilir sonucu atomik/checkpoint-consistent olur.

**Watermark neden gerekli?** Unbounded stream'de "bu event-time penceresi artık tamamlandı" kararı için ilerleme sinyali gerekir. Gecikme ile tamlık arasındaki business trade-off'u kodlar.

**State neden büyür?** Unbounded key cardinality, pencere retention, dedup TTL, skew ve cleanup davranışı. TTL/window lifecycle, backend, checkpoint size ve cardinality metriği birlikte yönetilir.

**Neden operator UID?** Savepoint/checkpoint state'ini yeni job graph'taki mantıksal operator ile kararlı biçimde eşlemek için; generated ID topology değişince kırılabilir.
