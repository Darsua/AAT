# Problem 3: Autentikasi dan otorisasi — bukti historis M1

**Konteks tanggal: 2026-10-08**, dari timestamp payload pada log auth. Log tidak memuat waktu mulai eksekusi lengkap; tanggal terpisah untuk uji cross-credential tidak tercatat. Bukti ini bukan validasi terkini atau jaminan M2. Tidak ada pengujian yang dijalankan ulang dalam cleanup dokumentasi ini.

## Eksekusi dan hasil yang tercatat

- [`cross_credential.log`](cross_credential.log), dari [`test_cross_credential.sh`](../../../scripts/test_cross_credential.sh): kredensial pada domain asal diterima (HTTP 200); kredensial BMKG ke PVMBG, PVMBG ke BMKG, dan akses tanpa kredensial ditolak (HTTP 401).
- [`auth_scopes.log`](auth_scopes.log), dari [`test_auth_scopes.sh`](../../../scripts/test_auth_scopes.sh): **15 passed, 0 failed**. Secret klien salah ditolak (401); token media memiliki `hazards:read:summary`, mendapat proyeksi summary (200), dan ditolak untuk `fields=all` (403). Token field/internal memiliki `hazards:read:all` dan berhasil mengakses data lengkap (200).
- Log auth mencatat TTL akses 60 detik, refresh berhasil dengan token baru, query memakai token baru berhasil (200), penggunaan ulang refresh token lama ditolak (401), dan replay mencabut sisa sesi refresh (401).

## Batas bukti

Refresh/revocation menggunakan memori proses; pengujian ini tidak membuktikan persistensi setelah restart atau konsistensi antar-replika. Pencabutan refresh tidak membatalkan JWT akses yang sudah diterbitkan sebelum kedaluwarsa. Konteks state dan batas M1 ada di [ADR 005](../../adr/005-stateless-architecture-and-m2-readiness.md).
