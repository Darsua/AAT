# Problem 5 Evidence: Pub-Sub Fan-Out, Consumer Downtime Recovery & Dynamic 3rd Consumer

Folder ini memuat log bukti eksekusi pengujian otomatis untuk Problem 5 pada sistem BNPB Disaster Data Aggregator.

## Berkas Bukti

1. [`consumer_recovery.log`](./consumer_recovery.log):
   - Verifikasi broker Redpanda dan topik `bnpb.hazard-events` dengan 3 partisi.
   - Penerbitan satu event bencana yang diterima secara paralel oleh dua consumer group independen (`bnpb-dashboard-group` dan `bnpb-notifier-group`).
   - Simulasi downtime kontainer `dashboard-consumer`: producer tetap mempublikasikan event tanpa tertahan, `notifier-consumer` tetap memproses secara real-time, dan Redpanda mencatat akumulasi lag pada grup Dashboard.
   - Pemulihan kontainer `dashboard-consumer`: membaca kembali dari committed offset, menyerap backlog yang tertinggal, hingga lag kembali ke 0.
   - Penambahan consumer group ketiga secara dinamis (`bnpb-evaluator-audit-group`) ke topik yang sama tanpa mengubah kode atau merestart Aggregator Service.
