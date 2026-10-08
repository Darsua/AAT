# Problem 3 Evidence: Autentikasi, Otorisasi, Trust Isolation, dan Token Lifecycle

Folder ini memuat log bukti eksekusi pengujian otomatis untuk Problem 3 pada sistem BNPB Disaster Data Aggregator.

## Berkas Bukti

1. [`cross_credential.log`](./cross_credential.log):
   - Bukti penolakan credential BMKG saat digunakan mengakses PVMBG (HTTP 401).
   - Bukti penolakan credential PVMBG saat digunakan mengakses BMKG (HTTP 401).
   - Bukti penolakan akses tanpa kredensial ke kedua endpoint upstream.
2. [`auth_scopes.log`](./auth_scopes.log):
   - Penerbitan JWT dengan TTL 60 detik (`exp` - `iat` = 60s).
   - Penolakan akses lintas scope (Client API menolak token tanpa scope `hazard:read`).
   - Token refresh dan rotasi single-use token.
   - Deteksi replay token penyegaran (reused refresh token langsung ditolak HTTP 401 dan memicu pencabutan sesi).
