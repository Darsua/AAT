# Audit ADR terhadap Kode dan Konfigurasi

Tanggal audit: 2026-10-10. Branch: `docs/report-p1-corrections`.
Cakupan: `docs/adr/001` sampai `005`, dibandingkan dengan kode, `docker-compose.yml`, `.env.example`, migrasi SQL, Dockerfile dan log bukti di `docs/evidence/`.

Aturan penilaian: kode dan konfigurasi adalah sumber kebenaran. Kode Go tidak diubah dalam audit ini. Yang diubah: ADR, bab laporan, `README.md` dan `services/aggregator/README.md`.

Notasi bukti: `[proof: berkas:baris]` berarti saya membaca baris itu pada sesi audit. `{unverified}` berarti tidak ada bukti di repo atau belum diperiksa. Nomor baris ADR merujuk ke versi **sebelum** perbaikan (commit `cd63221`), kecuali disebut lain.

Commit perbaikan:

| Commit | Isi |
| :--- | :--- |
| `2837f97` | Perbaikan ADR 001, 002, 003 |
| `b370934` | Perbaikan ADR 002, 004, 005 (bersama bab laporan) |
| `05bea3d` | Perbaikan lanjutan ADR 001 dan 004 |
| `df2151f` | Angka terukur di ADR 001 sampai 003, O4, O6, O7, O8, dokumen audit ini |
| `b1b2290` | O2, O9, O10 (ADR 001, README aggregator, README utama) |
| `0630e4a` | Pembersihan baris sisa di ringkasan audit |
| `73ac07c` | Bagian D dan F audit |
| `20355da` | Klaim MongoDB diuji; ADR 003 diperbaiki; bagian E6 |
| (commit ini) | Perbaikan dokumen audit (lihat bagian G) |

## Ringkasan

| Status | Jumlah |
| :--- | :--- |
| Salah dan sudah diperbaiki (F1 sampai F13) | 13 |
| Diperbaiki setelah diukur atau diputuskan (O1, O3, O4, O6, O7, O8) | 6 |
| Dicabut karena bukan kesalahan (O5) | 1 |
| Diperbaiki kemudian (O2, O9, O10) | 3 |

Temuan paling berat: ADR 005 mengklaim UUIDv7 padahal ID bersifat deterministik (F12), ADR 003 memuat SQL dan kode relay outbox yang usang (F4 sampai F6), dan hampir semua angka sumber daya di ADR 001 sampai 003 tidak cocok dengan pengukuran (O1 dan O3). Salah satunya mengubah alasan pemilihan: Redpanda tidak lebih hemat memori daripada Kafka KRaft dalam pengukuran ini (352 MiB berbanding 297 MiB).

---

## A. Sudah diperbaiki

### F1. ADR 001: jumlah kontainer salah
- **Klaim lama** (baris 51): "Deploying eight containers (six Go microservices, PostgreSQL 16, and Redpanda)".
- **Fakta:** `docker-compose.yml` mendefinisikan 10 layanan: `canonical-db`, `redpanda`, `redpanda-init`, `mock-bmkg`, `mock-pvmbg`, `aggregator`, `auth`, `client-api`, `dashboard-consumer`, `notifier-consumer` [proof: `docker-compose.yml:4,21,37,67,83,99,132,155,177,192`]. Layanan `consumers` berjalan sebagai dua kontainer, bukan satu.
- **Perbaikan:** kalimat menyebut 10 kontainer dan menjelaskan pembagiannya (`2837f97`).

### F2. ADR 001: "the other four build from a flat main.go"
- **Klaim lama** (baris 71): hanya empat layanan selain aggregator yang dibangun dari `main.go` di akar layanan.
- **Fakta:** enam layanan lain memakai `go build ... .`: `auth` [proof: `services/auth/Dockerfile:12`], `client-api` [proof: `services/client-api/Dockerfile:12`], `mock-bmkg` [proof: `services/mock-bmkg/Dockerfile:12`], `mock-pvmbg` [proof: `services/mock-pvmbg/Dockerfile:11`], `notifier-consumer` [proof: `services/consumers/notifier/Dockerfile:9`], `dashboard-consumer` [proof: `services/consumers/dashboard/Dockerfile:9`]. Hanya aggregator yang memakai `./cmd/server/main.go` [proof: `services/aggregator/Dockerfile:12`].
- **Perbaikan:** diganti "the other services" (`05bea3d`).

### F3. ADR 002: nama consumer group tidak sama dengan kode
- **Klaim lama** (baris 23, 37, 38, 80): `group.dashboard`, `group.notifier`.
- **Fakta:** nilai bawaan kode adalah `bnpb-dashboard-group` [proof: `services/consumers/dashboard/main.go:23`] dan `bnpb-notifier-group` [proof: `services/consumers/notifier/main.go:22`]. Compose menyetelnya lewat `CONSUMER_GROUP` [proof: `docker-compose.yml:181,196`], dan `.env.example` mendefinisikannya [proof: `.env.example:30-31`].
- **Perbaikan:** nama grup diganti dan catatan `CONSUMER_GROUP` ditambahkan (`2837f97`).

### F4. ADR 003: kunci pesan outbox salah
- **Klaim lama** (baris 213): `Key: []byte(eventID)`. ADR 002 sendiri menyatakan kunci = `hazard_id`, sehingga dua ADR saling bertentangan.
- **Fakta:** relay membaca `hazard_id` dari payload dan memakainya sebagai kunci [proof: `services/aggregator/internal/worker/outbox_relay.go:106`]. `event_id` outbox berasal dari `formatOutboxEventID(source, sourceRefID)`, bukan `hazard_id` [proof: `services/aggregator/internal/repository/hazard_repo.go:339`].
- **Perbaikan:** bagian relay ditulis ulang dengan kunci `hazard_id` (`2837f97`).

### F5. ADR 003: SQL yang diklaim "verbatim copy" ternyata berbeda
- **Klaim lama** (baris 97): blok SQL adalah salinan persis `services/aggregator/migrations/001_init.sql` dan "must never diverge".
- **Selisih** (hasil `diff` blok ADR terhadap berkas migrasi):
  - `hazard_id`, `source_ref_id` dan `event_id` bertipe `VARCHAR(64)` di ADR, tetapi `VARCHAR(128)` di migrasi [proof: `services/aggregator/migrations/001_init.sql`, kolom dan baris `ALTER ... TYPE VARCHAR(128)` pada baris 63].
  - Indeks `idx_hazards_severity_occurred` ada di migrasi dan tidak ada di ADR [proof: `001_init.sql:23`].
  - Kolom `correlation_id` dan `claim_expires_at` ada di migrasi dan tidak ada di ADR [proof: `001_init.sql:53,57,64,65`].
- **Perbaikan:** blok SQL diganti isi berkas migrasi yang sebenarnya (`2837f97`).

### F6. ADR 003: deskripsi dan kode relay outbox usang
Kode contoh di ADR (baris 181 sampai 237) tidak mencerminkan implementasi.

| Aspek | Klaim ADR | Fakta kode |
| :--- | :--- | :--- |
| Transaksi | Satu transaksi menahan `FOR UPDATE` selama menulis ke broker | Tiga fase: klaim singkat, publish di luar transaksi, penyelesaian singkat [proof: `hazard_repo.go:402,462,475`] |
| Pengaman klaim | Tidak ada | Lease 30 detik lewat `claim_expires_at` [proof: `hazard_repo.go:416,422`] |
| Record rusak | Tidak ditangani | Dikarantina: `event_type = 'hazard_event_quarantined'` [proof: `outbox_relay.go:89-96`, `hazard_repo.go:510`] |
| Header | Tidak ada | `X-Correlation-ID` selalu dikirim [proof: `outbox_relay.go:109`] |
| Penandaan terkirim | `WHERE id = ANY($1)` | Hanya jika payload tidak berubah: `WHERE id = $1 AND payload = $2::jsonb` [proof: `hazard_repo.go:490`] |
| Retry | "retries with backoff" (baris 237) | Interval tetap 500 ms tanpa backoff eksponensial [proof: `outbox_relay.go:46`] |

- **Perbaikan:** bagian "Transactional Outbox Worker Loop" ditulis ulang sesuai kode (`2837f97`).

### F7. ADR 004: rute klien salah
- **Klaim lama** (baris 8, 39, 92): `GET /hazards`.
- **Fakta:** rute publik adalah `GET /api/v1/hazards` dan `GET /api/v1/hazards/{id}` [proof: `services/client-api/main.go:172-173`].
- **Perbaikan:** ketiga kemunculan diganti (`05bea3d`).

### F8. ADR 004: diagram menyatakan client-api memanggil auth
- **Klaim lama** (baris 40): `ClientAPI->>Auth: Validate Token & Scopes (or local JWT verify)`.
- **Fakta:** `client-api` memverifikasi JWT secara lokal memakai `JWT_SECRET` [proof: `services/client-api/main.go:581-583`], dan tidak ada alamat atau port auth di berkasnya [proof: grep `8084|AUTH_URL|auth:` pada `services/client-api/main.go` tidak menghasilkan baris].
- **Perbaikan:** langkah diganti catatan verifikasi lokal dan partisipan `auth` dihapus dari diagram (`05bea3d`).

### F9. ADR 004: diagram menyebut `volcano_references` dibaca saat query
- **Klaim lama** (baris 43): `Aggregator->>DB: Query hazards + volcano_references`.
- **Fakta:** tabel katalog dimuat ke cache saat boot [proof: `services/aggregator/cmd/server/main.go:64`] dan dipakai oleh normalizer saat ingesti [proof: `services/aggregator/internal/normalizer/normalizer.go:102`]. Jalur kueri `GET /internal/hazards` hanya membaca `hazards`.
- **Verifikasi lanjutan (10 Okt):** tidak ada kemunculan `volcano_references` di kode Go selain `volcano_repo.go` [proof: grep `-rnF volcano_references services --include=*.go` di luar `volcano_repo.go` kosong], dan semua kueri di `hazard_repo.go` memakai `FROM hazards` tanpa JOIN [proof: `services/aggregator/internal/repository/hazard_repo.go:74,89,206,367,376`]. Keyakinan: tinggi.
- **Perbaikan:** diagram menjadi `Query hazards` (`05bea3d`).

### F10. ADR 004: klaim benchmark tidak berdasar dan angka pool bertentangan
- **Klaim lama** (baris 76): benchmark menunjukkan RTT sub-milidetik "<0.6 ms" dengan `MaxIdleConnsPerHost: 100`.
- **Fakta:** tidak ada benchmark atau berkas yang memuat angka 0,6 ms di repo [proof: grep `0\.6 ?ms|sub-millisecond` di luar `docs/adr` tidak menghasilkan baris]. Selain itu `MaxIdleConnsPerHost` adalah 50, bukan 100, di kode [proof: `services/client-api/main.go:144-145`] dan di blok kode ADR yang sama.
- **Perbaikan:** klaim diganti angka terukur: p95 1,75 ms dan p99 3,07 ms pada uji beban jalur penuh [proof: `docs/evidence/p2/load_test.log:205-206`], serta nilai pool 100 dan 50 (`05bea3d`).

### F11. ADR 004: antarmuka `HazardService` memuat `GetHealth`
- **Klaim lama:** antarmuka punya `GetHazards` dan `GetHealth`.
- **Fakta:** antarmuka di kode hanya punya `GetHazards` [proof: `services/client-api/main.go:128-130`].
- **Perbaikan:** `GetHealth` dihapus dari cuplikan (`b370934`).

### F12. ADR 005: klaim UUIDv7 salah
- **Klaim lama** (baris 17, 57 sampai 59, 143): `hazard_id` memakai UUIDv7 berurutan waktu, dibuat lokal tanpa round-trip ke database.
- **Fakta:** `hazard_id` adalah `<prefix>-<sourceRefID>` (misalnya `haz-bmkg-<event_id>`), diganti hash SHA-256 terpotong jika lebih dari 64 karakter [proof: `services/aggregator/internal/normalizer/normalizer.go:33-41,50,96`]. Contoh nyata di bukti: `haz-bmkg-bmkg-seis-live-20261008-095117-000309` [proof: `docs/evidence/p4/storage_restart.log:17`].
- **Perbaikan:** bagian "Time-Ordered Identifiers" diganti "Deterministic Identifiers" dengan trade-off yang jujur (tidak berurutan waktu), baris keputusan dan baris tabel ringkasan diperbarui (`b370934`).

### F13. ADR 005: kursor polling disebut disimpan transaksional di database
- **Klaim lama** (baris 55): "Polling cursors ... are committed transactionally to canonical-db."
- **Fakta:** kursor adalah field di memori poller dan di-bootstrap dari `occurred_at` terbaru lewat `GetLatestOccurredAt` saat start [proof: `services/aggregator/internal/worker/poller.go:64-70`]. Polling berikutnya membaca ulang dengan jendela tumpang tindih 10 detik [proof: `poller.go:18,105-108`], yang aman karena upsert idempoten `ON CONFLICT (source, source_ref_id)` [proof: `hazard_repo.go:165,313`].
- **Perbaikan:** dijelaskan ulang sesuai mekanisme sebenarnya (`b370934`).

### Tambahan kelengkapan (bukan kesalahan)
- ADR 002 tidak menyebut konfigurasi producer dan consumer. Ditambahkan: balancer `kafka.Hash`, `RequiredAcks: kafka.RequireOne`, `MaxAttempts: 3` [proof: `services/aggregator/cmd/server/main.go:89-97`], serta `FetchMessage`, `CommitMessages` manual dan `StartOffset: kafka.FirstOffset` [proof: `services/consumers/dashboard/main.go:147,183,195`] (`b370934`).

---

## B. Item yang tadinya terbuka

### O1. ADR 001: angka efisiensi sumber daya (DIPERBAIKI setelah diukur)
Hasil pengukuran ada di bagian E. Perbandingan dengan klaim lama (baris 53 sampai 60):

| Metrik | Klaim lama | Terukur | Cocok? |
| :--- | :--- | :--- | :--- |
| Ukuran biner | ~12 sampai 18 MB | 5,1 sampai 11,2 MB | Tidak |
| Ukuran image Go | < 25 MB | 24,4 sampai 32,6 MB (4 dari 7 di atas 25 MB) | Sebagian |
| Memori idle per layanan Go | ~10 sampai 16 MB | 1,8 sampai 16,6 MiB | Batas bawah salah |
| Total memori idle | < 100 MB (6 layanan) | 55,8 MiB (7 layanan) | Ya |
| Startup dingin Go | < 50 ms | 233 sampai 515 ms (sampai `/health` 200, termasuk `docker start`) | Tidak |
| Image Python | 180 sampai 250 MB | 178 MB (basis), 224 MB (dengan FastAPI) | Ya |
| Memori idle Python | 110 sampai 160 MB | ~32 MiB (hello-world) | Tidak |
| Startup Python | 1,2 sampai 2,5 s | 929 ms (hello-world) | Tidak |
| Total 6 layanan Python | 800 MB sampai 1,1 GB | tidak diukur (6 x 32 MiB = ~190 MiB hello-world) | Tidak |

- **Catatan:** pembanding Python adalah aplikasi hello-world, bukan layanan BNPB yang ditulis ulang, jadi angkanya batas bawah. Keunggulan Go tetap ada (rata-rata 55,8 / 7 = 8,0 MiB per layanan berbanding 31,7 MiB, sekitar 4x; image 224 / 32,6 = 6,9x sampai 224 / 24,4 = 9,2x lebih kecil) tetapi tidak sebesar klaim lama.
- **Perbaikan:** tabel dan paragraf ADR 001 diganti angka terukur dan klaim "memory bloat" dilunakkan.

### O2. ADR 001: cuplikan `HazardEvent` tidak lengkap (DIPERBAIKI)
- **Temuan:** struct di ADR hanya tujuh field (lima `string`, satu `time.Time`, satu `map`), tanpa tipe `Source`, `HazardType`, `Severity`, sedangkan struct asli bertipe `Source`, `HazardType`, `Severity` dan memuat `IngestedAt`, `SourceRefID`, `Latitude`, `Longitude` [proof: `services/aggregator/internal/domain/models.go:36-51`]. Kontrak mendefinisikan 11 properti [proof: `contracts/hazard_event.json`, dibaca dengan `python3`].
- **Perbaikan:** cuplikan ADR 001 diganti dengan 11 field asli dan diberi label "Excerpt of `models.go`".

### O3. ADR 002 dan 003: angka perbandingan teknologi (DIPERBAIKI setelah diukur)
Semua image dijalankan dengan pengaturan bawaan di mesin yang sama (bagian E).

| Teknologi | Klaim lama | Terukur | Cocok? |
| :--- | :--- | :--- | :--- |
| Redpanda, memori idle | 65 sampai 95 MB | 352,4 MiB (`--memory 256M`) | Tidak |
| Redpanda, startup sehat | < 1,5 s | 1,85 s | Tidak (sedikit) |
| Kafka 3.7 (KRaft), memori idle | 1,2 sampai 1,8 GB | 296,6 MiB | Tidak |
| Kafka 3.7, startup | 18 sampai 35 s | 3,9 s | Tidak |
| RabbitMQ 3.13, memori idle | 120 sampai 180 MB | 147,5 MiB | Ya |
| RabbitMQ 3.13, startup | 8 sampai 15 s | 5,2 s (log "Time to start RabbitMQ") | Tidak |
| NATS 2.10, memori idle | 25 sampai 40 MB | 7,4 MiB | Tidak |
| PostgreSQL 16, memori idle | 22 sampai 28 MB | 47,7 MiB | Tidak |
| MongoDB 7.0, memori idle | 350 sampai 650 MB | 76,2 MiB | Tidak |
| MySQL 8.0, memori idle | 180 sampai 300 MB | 385,2 MiB | Tidak |

- **Dampak pada alasan keputusan:** argumen memori melawan Kafka dan MongoDB tidak berlaku. Redpanda memakai lebih banyak memori daripada Kafka dalam pengukuran ini (352,4 MiB dengan `--memory 256M` berbanding 296,6 MiB). Alasan yang tetap sah: kompatibilitas protokol Kafka, biner tunggal tanpa JVM, `rpk` dipakai oleh `redpanda-init` [proof: `docker-compose.yml:32,42-45`], dan untuk MongoDB kebutuhan replica set untuk transaksi multi-dokumen. Klaim terakhir diuji dan benar, kecuali frasa "multi-node" yang salah karena satu anggota cukup [proof: bagian E6].
- **Perbaikan:** tabel dan paragraf ADR 002 dan 003 diganti angka terukur, alasan Kafka dan MongoDB ditulis ulang, dan judul poin 1 ADR 002 diubah menjadi "Without a JVM Runtime".

### O4. ADR 002: nama image Redpanda (DIPERBAIKI)
- Diganti `docker.redpanda.com/redpandadata/redpanda:v24.1.1` sesuai compose [proof: `docker-compose.yml:22,38`].

### O5. ADR 002: header korelasi disebut "optional" (DICABUT, bukan kesalahan)
- **Klaim:** envelope pesan punya "optional `X-Correlation-ID` header".
- **Pemeriksaan:** kontrak beku yang dirujuk ADR menulis hal yang sama: "`X-Correlation-ID` (string, optional)" [proof: `services/aggregator/README.md:74,135`]. Kode selalu mengirim header ini [proof: `outbox_relay.go:100-109`], yang tidak melanggar kata "optional" dari sisi konsumen.
- **Status:** tidak ada tindakan.

### O6. ADR 002: nama grup konsumen ketiga (DIPERBAIKI)
- ADR 002 sekarang merujuk `bnpb-evaluator-audit-group` dari skrip [proof: `scripts/test_consumer_recovery.sh:26`] dan menjelaskan padanan `auto_offset_reset: "earliest"` dengan `kafka.FirstOffset`.

### O7. ADR 002: kode deduplikasi hanya ilustrasi (DIPERBAIKI)
- Ditambahkan catatan "Milestone 1 status": implementasi memakai `map[string]struct{}` di memori [proof: `services/consumers/dashboard/main.go:63,70`, `services/consumers/notifier/main.go:64,68`], direset saat restart, dan Redis atau tabel unik baru menjadi syarat Milestone 2.

### O8. ADR 005: rencana Milestone 2 (DIPERBAIKI: diberi label rencana)
- Bagian "Milestone 2 Evolution Path" kini diawali catatan "Status: plan only". Catatan kunci partisi juga ditambahkan: M1 memakai kunci `hazard_id` (urutan per kejadian), sedangkan rencana M2 mengubahnya menjadi per kategori atau area sehingga jaminan urutan berubah.

### O9. README aggregator memuat SQL usang yang sama dengan ADR 003 (DIPERBAIKI)
- **Temuan:** `diff` antara blok SQL README dan `migrations/001_init.sql` menunjukkan perbedaan: `VARCHAR(64)` di README versus `VARCHAR(128)` di migrasi untuk `hazard_id`, `source_ref_id`, `event_id`; kolom `correlation_id` dan `claim_expires_at`, indeks `idx_hazards_severity_occurred`, data seed enam gunung, dan `ALTER TABLE` idempoten tidak ada di README [proof: keluaran `diff` sesi ini].
- **Klaim lain yang salah di README yang sama:** "circuit breaking" dan "circuit breaker" (kode tidak punya; hanya status tracker, lihat `internal/status/tracker.go`) [proof: `grep -rniE "circuit|jitter" services/aggregator` hanya menemukan komentar dan `randomJitter` di `poller.go:77`].
- **Perbaikan:** blok SQL diganti isi `001_init.sql` apa adanya; klaim circuit breaker diganti deskripsi status tracker; pool 25/5 sudah benar [proof: `config.go:32,37`].

### O10. Port host 8083 aggregator tidak terbuka (DIPERBAIKI di dokumentasi)
- **Gejala:** `curl http://127.0.0.1:8083/health` menghasilkan kode 000, sedangkan `127.0.0.1:8080/health` menghasilkan 200 [proof: keluaran curl sesi ini].
- **Sebab:** compose mendefinisikan `127.0.0.1:8083:8083` [proof: `docker-compose.yml:123`], tetapi aggregator hanya terhubung ke `bnpb-internal` yang bertanda `internal: true` [proof: `docker-compose.yml:211-213`; `docker network inspect bnpb-aat_bnpb-internal --format '{{.Internal}}'` = `true`]. Docker tidak mempublikasikan port pada jaringan internal: `docker inspect` menunjukkan `NetworkSettings.Ports` = `{"8083/tcp":null}` dan `docker ps` hanya menampilkan `8083/tcp`, sedangkan `client-api` (jaringan publik) menampilkan `0.0.0.0:8080->8080/tcp` [proof: keluaran `docker ps` dan `docker inspect` sesi ini].
- **Dampak:** perilaku sesuai desain isolasi (P4: aggregator tidak terjangkau dari luar). Yang salah adalah dokumentasi: README utama baris 115 menyebut "Host Loopback" dan README aggregator menyarankan `curl localhost:8083`. Laporan sudah menulis "port 8083 internal" [proof: `docs/report/chapters/02_arsitektur.tex:7`], jadi tidak perlu diubah.
- **Perbaikan:** README utama dan README aggregator dikoreksi. Baris `ports:` di `docker-compose.yml` tidak diubah karena tidak berpengaruh; menghapusnya adalah keputusan Anda.

## C. Klaim yang diperiksa dan cocok

| Klaim | Bukti |
| :--- | :--- |
| Go 1.22 pada semua modul dan `go.work` mencakup tujuh modul | [proof: `go.work`, `services/*/go.mod` semuanya `go 1.22`] |
| `pgx/v5` hanya di aggregator, `kafka-go` di aggregator dan dua consumer, `golang-jwt/v5` di auth dan client-api | [proof: grep `go.mod` setiap layanan] |
| Pola Dockerfile multi-tahap, `alpine:3.20`, `ca-certificates tzdata`, `USER nobody:nobody` | [proof: `services/aggregator/Dockerfile:2-29`] |
| Topik `bnpb.hazard-events` dengan 3 partisi dari `redpanda-init` | [proof: `docker-compose.yml:59` (`KAFKA_TOPIC_PARTITIONS:-3`)] |
| `canonical-db` tidak mengekspos port ke host dan hanya berada di jaringan internal | [proof: `docker-compose.yml:4-14`] |
| Filter query aggregator: `since`, `type`, `severity`, `limit`, `offset` | [proof: `services/aggregator/internal/api/hazard_handler.go:81,97,111,127,141`] |
| Parameter `http.Transport` client-api (timeout 5 s, 100/50/90 s) | [proof: `services/client-api/main.go:142-146`] |
| Idempotensi `UNIQUE (source, source_ref_id)` dan upsert | [proof: `hazard_repo.go:165,313`] |
| Pengecualian M1: refresh token di memori, dedup konsumen di memori | [proof: `services/auth/main.go:101-108`, `dashboard/main.go:63`] |
| Respons degradasi punya `degraded` dan `stale_since` | [proof: `hazard_handler.go:30-31`] |

## D. Cara memverifikasi ulang

```bash
# Daftar layanan compose (F1): 10 layanan; grep penuh menghasilkan 14 baris karena menyertakan 2 volume dan 2 jaringan
sed -n '/^services:/,/^volumes:/p' docker-compose.yml | grep -cE '^  [a-z0-9-]+:$'
# Kunci pesan dan header relay (F4, F6)
grep -nE 'Key:|X-Correlation-ID|quarantin' services/aggregator/internal/worker/outbox_relay.go
# Rumus hazard_id (F12)
sed -n 33,50p services/aggregator/internal/normalizer/normalizer.go
# SQL ADR vs migrasi (F5)
diff <(awk '/^```sql/{f=1;next} /^```/{f=0} f' docs/adr/003-canonical-storage-postgres-jsonb.md) services/aggregator/migrations/001_init.sql
# SQL README aggregator vs migrasi (O9), jalankan dengan bash, bukan fish
diff <(awk '/^```sql/{f=1;next} /^```/{f=0} f' services/aggregator/README.md) services/aggregator/migrations/001_init.sql
# Port aggregator tidak dipublikasikan (O10)
docker compose up -d && docker ps --format '{{.Names}} {{.Ports}}' | grep aggregator
docker network inspect bnpb-aat_bnpb-internal --format '{{.Internal}}'
# Uji transaksi MongoDB (E6), perlu image mongo:7.0
bash docs/evidence/adr-audit/mongo_tx_test.sh
# Ukur ulang sumber daya (O1, bagian E1 sampai E3)
docker compose up -d --build && sleep 120 && docker stats --no-stream --format '{{.Name}} {{.MemUsage}}'
docker images --format '{{.Repository}} {{.Size}}' | grep bnpb
```

Catatan: `docker compose up` membutuhkan berkas `.env` (salin dari `.env.example`). Ukuran pembanding pihak ketiga (E4) diambil dengan `docker run` image bawaan tanpa konfigurasi dan memori dibaca dengan `docker stats --no-stream` 45 detik setelah siap.

## E. Data pengukuran (10 Oktober 2026)

**Lingkungan:** Docker 29.8.2 pada CachyOS [proof: `docker info --format "{{.ServerVersion}} {{.OperatingSystem}}"` menampilkan `29.8.2 CachyOS`], RAM host 13,49 GiB [proof: kolom LIMIT pada `docker stats`]. Satu sampel per angka. Tumpukan dijalankan dengan `docker compose up -d --build` dari branch `docs/report-p1-corrections` dan diukur sekitar 2 menit setelah semua layanan sehat.

### E1. Layanan BNPB (idle, `docker stats --no-stream`)

| Kontainer | Memori |
| :--- | :--- |
| aggregator | 16,62 MiB |
| mock-pvmbg | 9,738 MiB |
| mock-bmkg | 8,926 MiB |
| notifier-consumer | 7,828 MiB |
| dashboard-consumer | 7,809 MiB |
| client-api | 3,105 MiB |
| auth | 1,82 MiB |
| **Jumlah 7 layanan Go** | **55,8 MiB** |
| canonical-db (postgres:16-alpine) | 47,74 MiB |
| redpanda | 352,4 MiB |

### E2. Ukuran image dan biner (`docker images`, `stat`)

| Layanan | Image | Biner |
| :--- | :--- | :--- |
| mock-bmkg | 24,4 MB | 5,06 MB |
| mock-pvmbg | 24,5 MB | 5,07 MB |
| auth | 24,6 MB | 5,14 MB |
| dashboard-consumer | 25,2 MB | 5,62 MB |
| notifier-consumer | 25,2 MB | 5,63 MB |
| aggregator | 32,5 MB | 11,15 MB |
| client-api | 32,6 MB | 5,62 MB |
| postgres:16-alpine | 420 MB | tidak berlaku |
| redpanda v24.1.1 | 581 MB | tidak berlaku |

### E3. Startup dingin (`docker stop` lalu `docker start`, polling tiap 10 ms)

| Layanan | Waktu | Kriteria siap |
| :--- | :--- | :--- |
| mock-pvmbg | 233 ms | `/health` 200 dari host |
| mock-bmkg | 250 ms | `/health` 200 dari host |
| client-api | 272 ms | `/health` 200 dari host |
| aggregator | 304 ms | `/health` 200 dari dalam kontainer (port host 8083 tidak menjawab dari host) |
| auth | 515 ms | `/health` 200 dari host |
| canonical-db | 542 ms | `pg_isready` |
| redpanda | 1854 ms | `rpk cluster health` = sehat |

Waktu mencakup overhead `docker start`, jadi bukan waktu proses semata.

### E4. Pembanding pihak ketiga (image bawaan, tanpa konfigurasi khusus, 45 detik setelah siap)

| Image | Ukuran image | Siap setelah | Memori idle |
| :--- | :--- | :--- | :--- |
| mongo:7.0 | 1,18 GB | 1240 ms | 76,22 MiB |
| mysql:8.0 | 1,1 GB | 5411 ms | 385,2 MiB |
| rabbitmq:3.13 | 352 MB | 5178 ms (log RabbitMQ) | 147,5 MiB |
| nats:2.10 (`-js`) | 22,4 MB | tidak diukur presisi | 7,398 MiB |
| apache/kafka:3.7.0 | 613 MB | 3887 ms | 296,6 MiB |
| FastAPI hello-world (python:3.12-slim + fastapi + uvicorn) | 224 MB (basis 178 MB) | 929 ms | 31,69 MiB |

### E5. Batasan pengukuran
- Satu sampel, tanpa pengulangan; angka memori dapat bergeser antar-lari.
- Percobaan pertama cek kesiapan RabbitMQ gagal karena perintah cek saya, bukan karena RabbitMQ; angka RabbitMQ diambil dari lari ulang.
- Startup dingin mencakup `docker start`. Klaim "< 50 ms" ADR 001 hanya benar jika yang diukur adalah proses saja, dan itu tidak diukur.
- Pembanding Python dan pihak ketiga memakai pengaturan bawaan dan tidak mewakili konfigurasi produksi.
- Di sesi ini saya mengubah state mesin Anda: menyalakan layanan Docker, membuat `.env` (diabaikan git), mem-build image, dan menjalankan 9 kontainer. Setelah pengukuran, stack dimatikan dengan `docker compose down` dan `docker ps` menampilkan nol kontainer berjalan. Image hasil build dan image pembanding masih ada di cache Docker. Tiga kontainer berstatus Exited (`agitated_curran`, `upbeat_wozniak`, `condescending_greider`, image sama, umur 2 hari) bukan dari sesi ini dan tidak disentuh.

### E6. Uji transaksi MongoDB 7.0 (`mongo:7.0`, skrip sesi ini)

| Skenario | Hasil |
| :--- | :--- |
| A. mongod standalone, transaksi lewat `mongosh` | Gagal: "This MongoDB deployment does not support retryable writes" (pesan dari sisi sesi `mongosh`, bukan pesan transaksi) |
| A'. mongod standalone, perintah `insert` mentah dengan `txnNumber` dan `startTransaction: true` | Gagal: `MongoServerError: Transaction numbers are only allowed on a replica set member or mongos` (sama persis dengan kutipan ADR 003) |
| B. `--replSet rs0` sebelum `rs.initiate()` | Gagal: `NotWritablePrimary | not primary` |
| C. replica set **satu anggota** setelah `rs.initiate()` | Berhasil: transaksi dua dokumen `TX_OK` |

Kesimpulan: klaim "memerlukan replica set" benar dan teks error di ADR benar apa adanya (A'). Klaim "multi-node replica set" salah: satu anggota cukup (C). Ketergantungan `rs.initiate()` sebelum transaksi juga nyata (B). ADR 003 diperbaiki sesuai ini. Batas: satu sampel, hanya MongoDB 7.0, tanpa uji driver Go.

## F. Status akhir

| Kategori | Jumlah | Rincian |
| :--- | :--- | :--- |
| Salah dan diperbaiki | 13 | F1 sampai F13 |
| Diperbaiki setelah diukur atau diputuskan | 6 | O1, O3, O4, O6, O7, O8 |
| Diperbaiki kemudian | 3 | O2, O9, O10 |
| Dicabut | 1 | O5 |
| Terbuka | 0 | |

Keputusan yang tersisa untuk Anda:
1. Baris `ports: "127.0.0.1:8083:8083"` pada aggregator di `docker-compose.yml` tidak berpengaruh (O10). Boleh dihapus atau dibiarkan.
2. Angka pada E4 dan pembanding Python berasal dari satu sampel dan pengaturan bawaan. Jika ADR dijadikan dasar nilai, ulangi pengukuran beberapa kali dengan perintah di bagian D.

## G. Perbaikan dokumen audit ini (10 Oktober, setelah tinjauan ulang)

| Perbaikan | Alasan |
| :--- | :--- |
| Tag `{unverified}` untuk MongoDB di O3 dihapus dan diganti rujukan ke E6 | Klaim sudah diuji |
| O2: "tujuh field bertipe string" dikoreksi | Dari tujuh field, lima `string`, satu `time.Time`, satu `map` |
| O1: rumus "~4x" dan "7x sampai 9x" ditulis eksplisit | Angka sebelumnya tanpa perhitungan |
| O3: pasangan 352,4 versus 296,6 MiB disebut di kalimat dampak | Temuan utama tadinya hanya ada di tabel |
| O10: ditambah pembanding port `client-api` | Bukti bahwa perbedaan ada pada jaringan, bukan pada Docker secara umum |
| D: perintah hitung layanan dibatasi ke blok `services:` | Perintah lama menghasilkan 14 baris (10 layanan, 2 volume, 2 jaringan) dan dapat disalahbaca sebagai 14 layanan [proof: `grep -nE` sesi ini] |
| D: perintah `diff` F5 dan O9 dijalankan ulang | Keduanya tidak menghasilkan selisih (ADR 003 dan README aggregator kini identik dengan migrasi) [proof: `diff` sesi ini] |
| Skrip uji MongoDB disimpan di `docs/evidence/adr-audit/mongo_tx_test.sh` | Agar E6 dapat diulang |
