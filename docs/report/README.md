# Laporan Modular LaTeX — Milestone 1 (IF4031 Arsitektur Aplikasi Terdistribusi)

Direktori ini memuat naskah laporan Milestone 1 sistem koordinasi kebencanaan BNPB dalam format LaTeX yang terstruktur secara modular. Setiap anggota tim dapat mengedit berkas bab atau permasalahan yang ditugaskan secara terisolasi tanpa risiko konflik Git (*merge conflict*).

---

## 1. Struktur Modul dan Pembagian Berkas

```
docs/report/
├── main.tex                       # Master document yang memanggil seluruh modul
├── preamble.tex                   # Konfigurasi geometri (4-3-3-3 cm), paket, styling listing & tabel
├── references.bib                 # Daftar pustaka BibTeX
├── chapters/
│   ├── 00_cover.tex               # Halaman sampul (Kelompok AhhAldoyTolong, identitas anggota, STEI ITB)
│   ├── 00_frontmatter.tex         # Daftar Isi, Daftar Tabel, Daftar Gambar, dan Tabel Kontribusi (Tabel 0.1)
│   ├── 01_pendahuluan.tex         # Bab 1: Deskripsi umum sistem, tujuan, dan asumsi perancangan
│   ├── 02_arsitektur.tex          # Bab 2: Topologi arsitektur, boundary service, dan Tabel 2.6.1 Teknologi
│   ├── 03_solusi_p1.tex           # Problem 1: Evolusi skema dinamis PVMBG (Aldoy)
│   ├── 03_solusi_p2.tex           # Problem 2: Konkurensi goroutine, degradasi, uji beban k6 (Darrel)
│   ├── 03_solusi_p3.tex           # Problem 3: Isolasi trust domain, RBAC scope, token refresh (Jenka)
│   ├── 03_solusi_p4.tex           # Problem 4: Isolasi container, evaluasi 3-way storage, outbox (Darrel)
│   ├── 03_solusi_p5.tex           # Problem 5: Pub-sub Redpanda, downtime recovery, penambahan consumer (Jenka)
│   └── 04_petunjuk.tex            # Bab 4: Petunjuk menjalankan sistem dan matriks verifikasi pengujian
├── appendix/
│   └── ai_declaration.tex         # Lampiran A: Formulir pernyataan penggunaan AI
└── figures/                       # Tangkapan layar / aset visual (jika ada)
```

---

## 2. Cara Mengompilasi Laporan

Kompilasi dilakukan menggunakan mesin kompilasi modern [`tectonic`](https://tectonic-typesetting.github.io/), yang mengunduh dependensi paket TeX secara otomatis tanpa konfigurasi rumit.

### Menggunakan Makefile (Direktori Utama Repositori)
```bash
make report
```

Perintah di atas akan menghasilkan dua berkas PDF:
1. `docs/report/main.pdf` (berkas keluaran kompilasi standar).
2. `docs/report/IF4031_M1_AhhAldoyTolong.pdf` (berkas keluaran siap kumpul sesuai format penamaan wajib pada spesifikasi).

---

## 3. Pedoman Penulisan untuk Anggota Tim

1. **Struktur Setiap Problem (P1–P5):**  
   Setiap berkas `03_solusi_pX.tex` wajib mempertahankan kelima subbab berikut sesuai rubrik penilaian spesifikasi:
   - `3.x.1 Rancangan Solusi dan Mekanisme`
   - `3.x.2 Alasan Keputusan dan Trade-Off`
   - `3.x.3 Batasan yang Disadari`
   - `3.x.4 Bukti Penyelesaian`
   - `3.x.5 Jawaban Butir "Di Laporan"`
2. **Karakter Khusus LaTeX:**  
   - Karakter garis bawah (`_`) di luar lingkungan kode/listing harus di-escape (`\_`).
   - Karakter ampersand (`&`) di luar lingkungan tabel harus di-escape (`\&`).
3. **Potongan Kode:**  
   Gunakan blok `\begin{lstlisting}[language=...] ... \end{lstlisting}`. Konfigurasi `preamble.tex` telah mendukung `bash`, `Go`, `SQL`, dan `json`.
