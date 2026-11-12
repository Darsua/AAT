# Problem 5: Fan-out dan pemulihan consumer — bukti historis M1

**Konteks tanggal: 2026-10-08**, diturunkan dari epoch run ID `1791471484` pada log dan penggunaan `date +%s` di skrip, bukan timestamp payload sintetis. Bukti ini bukan validasi terkini atau jaminan M2. Tidak ada pengujian yang dijalankan ulang dalam cleanup dokumentasi ini.

## Eksekusi dan hasil yang tercatat

[`consumer_recovery.log`](consumer_recovery.log), dari [`test_consumer_recovery.sh`](../../../scripts/test_consumer_recovery.sh), mencatat **11 passed, 0 failed**:

- Redpanda merespons dan topik `bnpb.hazard-events` memiliki **3 partisi**.
- Event yang sama tercatat pada dua grup independen: `bnpb-dashboard-group` dan `bnpb-notifier-group`.
- Saat Dashboard dihentikan, Notifier memproses event downtime dan lag grup Dashboard mencapai **5**.
- Setelah Dashboard dijalankan kembali, event yang tertinggal diproses dan lag turun menjadi **0**.
- Grup ketiga, `bnpb-evaluator-audit-group`, menerima event tanpa perubahan atau restart publisher/Aggregator.

## Batas bukti

Skrip menerbitkan event uji **langsung ke broker** memakai `rpk`; ini membuktikan jalur broker/consumer yang diamati, bukan ingestion atau relay outbox end-to-end. Pemulihan bergantung pada retensi pesan dan committed offset yang masih tersedia. Uji ini tidak membuktikan ketahanan kehilangan node, exactly-once effects, atau pemrosesan revisi hazard: deduplikasi hazard-ID dan counter consumer hanya tersimpan di memori proses. Lihat [ADR 002](../../adr/002-message-broker-redpanda.md) dan [ADR 005](../../adr/005-stateless-architecture-and-m2-readiness.md).
