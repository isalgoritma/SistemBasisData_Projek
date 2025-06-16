#!/usr/bin/env python3
"""
SIPATANI - Sistem Informasi Pertanian
Fitur 1: Manajemen Jadwal Tanam
Database: PostgreSQL
"""

import psycopg2
from psycopg2 import sql
import datetime
from typing import List, Dict, Any, Optional
import os
from tabulate import tabulate

class SipataniDatabase:
    def __init__(self):
        self.connection = None
        self.cursor = None
        
    def connect(self):
        print("Koneksi ke database PostgreSQL")
        try:
            self.connection = psycopg2.connect(
                host="localhost",
                database="sipatani",
                user="postgres",  # Ganti username PostgreSQL 
                password="ardan230202",  # Ganti password PostgreSQL 
                port="5432"
            )
            self.cursor = self.connection.cursor()
            print("Koneksi ke database berhasil!")
            return True
        except psycopg2.Error as e:
            print(f"Error koneksi database: {e}")
            return False
    
    def disconnect(self):
        print("Tutup koneksi database")
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Koneksi database ditutup")

class JadwalTanamManager:
    def __init__(self, db: SipataniDatabase):
        self.db = db
    
    def lihat_semua_jadwal(self) -> List[Dict[str, Any]]:
        print("Fitur 1.1: Lihat Semua Jadwal dengan jarak tanaman")
        try:
            query = """
            SELECT 
                jt.id_jadwal_tanam,
                jt.id_lahan,
                l.id_petani,
                jt.id_tanaman,
                t.nama_tanaman,
                t.jarak_antar_tanaman,
                l.jumlah_pegawai,
                jt.tanggal as tanggal_tanam,
                t.durasi_tanam,
                s.status as status_jadwal
            FROM Jadwal_Tanam jt
            JOIN Lahan l ON jt.id_lahan = l.id_lahan
            JOIN Tanaman t ON jt.id_tanaman = t.id_tanaman
            JOIN Status_Jadwal s ON jt.status_jadwal_id = s.id_status_jadwal
            ORDER BY jt.id_jadwal_tanam;
            """
            
            self.db.cursor.execute(query)
            results = self.db.cursor.fetchall()
            
            columns = [
                "ID Jadwal", "ID Lahan", "ID Petani", "ID Tanaman", 
                "Nama Tanaman", "Jarak Tanaman (cm)", "Jumlah Pegawai", 
                "Tanggal Tanam", "Durasi (hari)", "Status Jadwal"
            ]
            
            if results:
                print("\nSEMUA JADWAL TANAM")
                print("=" * 80)
                print(tabulate(results, headers=columns, tablefmt="grid"))
                return [dict(zip([col.lower().replace(' ', '_') for col in columns], row)) for row in results]
            else:
                print("Tidak ada jadwal tanam yang ditemukan")
                return []
                
        except psycopg2.Error as e:
            print(f"Error mengambil data jadwal: {e}")
            return []
    
    def get_tanaman_list(self) -> List[Dict[str, Any]]:
        print("Ambil daftar tanaman yang tersedia")
        try:
            query = "SELECT id_tanaman, nama_tanaman, durasi_tanam, jarak_antar_tanaman FROM Tanaman ORDER BY nama_tanaman;"
            self.db.cursor.execute(query)
            results = self.db.cursor.fetchall()
            return [{"id": row[0], "nama": row[1], "durasi": row[2], "jarak": row[3]} for row in results]
        except psycopg2.Error as e:
            print(f"Error mengambil daftar tanaman: {e}")
            return []
    
    def get_lahan_list(self) -> List[Dict[str, Any]]:
        print("Ambil daftar lahan yang tersedia")
        try:
            query = """
            SELECT l.id_lahan, l.luas_lahan, l.jumlah_pegawai, p.nama_petani 
            FROM Lahan l
            JOIN Petani p ON l.id_petani = p.id_petani
            ORDER BY l.id_lahan;
            """
            self.db.cursor.execute(query)
            results = self.db.cursor.fetchall()
            return [{"id": row[0], "luas": row[1], "pegawai": row[2], "petani": row[3]} for row in results]
        except psycopg2.Error as e:
            print(f"Error mengambil daftar lahan: {e}")
            return []
    
    def get_next_jadwal_id(self) -> int:
        print("Ambil ID jadwal tanam berikutnya")
        try:
            query = "SELECT COALESCE(MAX(id_jadwal_tanam), 8000) + 1 FROM Jadwal_Tanam;"
            self.db.cursor.execute(query)
            result = self.db.cursor.fetchone()
            return result[0] if result else 8001
        except psycopg2.Error as e:
            print(f"Error mengambil ID berikutnya: {e}")
            return 8001
    
    def tambah_jadwal_baru(self) -> bool:
        print("Fitur 1.2: Tambah Jadwal Baru")
        try:
            print("\nTAMBAH JADWAL TANAM BARU")
            print("=" * 40)
            
            # Tampilkan daftar tanaman
            tanaman_list = self.get_tanaman_list()
            if not tanaman_list:
                print("Tidak ada data tanaman")
                return False
            
            print("\nDAFTAR TANAMAN:")
            for t in tanaman_list:
                print(f"ID: {t['id']} | {t['nama']} | Durasi: {t['durasi']} hari | Jarak: {t['jarak']} cm")
            
            # Input ID tanaman
            while True:
                try:
                    id_tanaman = int(input("\nPilih ID Tanaman: "))
                    tanaman_dipilih = next((t for t in tanaman_list if t['id'] == id_tanaman), None)
                    if tanaman_dipilih:
                        break
                    else:
                        print("ID tanaman tidak valid!")
                except ValueError:
                    print("Masukkan angka yang valid!")
            
            # Tampilkan daftar lahan
            lahan_list = self.get_lahan_list()
            if not lahan_list:
                print("Tidak ada data lahan")
                return False
            
            print(f"\nDAFTAR LAHAN:")
            for l in lahan_list:
                print(f"ID: {l['id']} | Luas: {l['luas']} m² | Pegawai: {l['pegawai']} | Petani: {l['petani']}")
            
            # Input ID lahan
            while True:
                try:
                    id_lahan = int(input("\nPilih ID Lahan: "))
                    lahan_dipilih = next((l for l in lahan_list if l['id'] == id_lahan), None)
                    if lahan_dipilih:
                        break
                    else:
                        print("ID lahan tidak valid!")
                except ValueError:
                    print("Masukkan angka yang valid!")
            
            # Input tanggal tanam
            while True:
                try:
                    tanggal_str = input("\nTanggal Tanam (YYYY-MM-DD): ")
                    tanggal_tanam = datetime.datetime.strptime(tanggal_str, "%Y-%m-%d").date()
                    break
                except ValueError:
                    print("Format tanggal salah! Gunakan YYYY-MM-DD")
            
            # Generate ID jadwal baru
            id_jadwal_baru = self.get_next_jadwal_id()
            
            # Status default: Terjadwal (993)
            status_id = 993
            
            # Insert ke database
            query = """
            INSERT INTO Jadwal_Tanam (id_jadwal_tanam, tanggal, id_lahan, id_tanaman, status_jadwal_id)
            VALUES (%s, %s, %s, %s, %s);
            """
            
            self.db.cursor.execute(query, (id_jadwal_baru, tanggal_tanam, id_lahan, id_tanaman, status_id))
            self.db.connection.commit()
            
            print(f"\nJadwal tanam berhasil ditambahkan!")
            print(f"ID Jadwal: {id_jadwal_baru}")
            print(f"Tanaman: {tanaman_dipilih['nama']}")
            print(f"Lahan: {id_lahan}")
            print(f"Tanggal Tanam: {tanggal_tanam}")
            print(f"Durasi: {tanaman_dipilih['durasi']} hari")
            print(f"Jarak Antar Tanaman: {tanaman_dipilih['jarak']} cm")
            
            return True
            
        except psycopg2.Error as e:
            print(f"Error menambah jadwal: {e}")
            self.db.connection.rollback()
            return False
    
    def edit_jadwal(self) -> bool:
        print("Fitur 1.3: Edit Jadwal")
        try:
            print("\nEDIT JADWAL TANAM")
            print("=" * 30)
            
            # Tampilkan jadwal yang ada
            jadwal_list = self.lihat_semua_jadwal()
            if not jadwal_list:
                return False
            
            # Pilih jadwal yang akan diedit
            while True:
                try:
                    id_jadwal = int(input("\nMasukkan ID Jadwal yang akan diedit: "))
                    
                    # Cek apakah jadwal ada
                    query = "SELECT * FROM Jadwal_Tanam WHERE id_jadwal_tanam = %s;"
                    self.db.cursor.execute(query, (id_jadwal,))
                    jadwal = self.db.cursor.fetchone()
                    
                    if jadwal:
                        break
                    else:
                        print("ID jadwal tidak ditemukan!")
                except ValueError:
                    print("Masukkan angka yang valid!")
            
            print(f"\nMengedit jadwal ID: {id_jadwal}")
            print("Pilih yang ingin diedit:")
            print("1. Tanggal Tanam")
            print("2. ID Lahan") 
            print("3. ID Tanaman")
            print("4. Status Jadwal")
            
            pilihan = input("\nPilihan (1-4): ")
            
            if pilihan == "1":
                while True:
                    try:
                        tanggal_baru = input("Tanggal baru (YYYY-MM-DD): ")
                        tanggal_obj = datetime.datetime.strptime(tanggal_baru, "%Y-%m-%d").date()
                        
                        query = "UPDATE Jadwal_Tanam SET tanggal = %s WHERE id_jadwal_tanam = %s;"
                        self.db.cursor.execute(query, (tanggal_obj, id_jadwal))
                        self.db.connection.commit()
                        print("Tanggal berhasil diupdate!")
                        return True
                    except ValueError:
                        print("Format tanggal salah!")
            
            elif pilihan == "2":
                lahan_list = self.get_lahan_list()
                print("\nDAFTAR LAHAN:")
                for l in lahan_list:
                    print(f"ID: {l['id']} | Luas: {l['luas']} m² | Pegawai: {l['pegawai']}")
                
                try:
                    id_lahan_baru = int(input("ID Lahan baru: "))
                    query = "UPDATE Jadwal_Tanam SET id_lahan = %s WHERE id_jadwal_tanam = %s;"
                    self.db.cursor.execute(query, (id_lahan_baru, id_jadwal))
                    self.db.connection.commit()
                    print("ID Lahan berhasil diupdate!")
                    return True
                except ValueError:
                    print("Masukkan angka yang valid!")
            
            elif pilihan == "3":
                tanaman_list = self.get_tanaman_list()
                print("\nDAFTAR TANAMAN:")
                for t in tanaman_list:
                    print(f"ID: {t['id']} | {t['nama']}")
                
                try:
                    id_tanaman_baru = int(input("ID Tanaman baru: "))
                    query = "UPDATE Jadwal_Tanam SET id_tanaman = %s WHERE id_jadwal_tanam = %s;"
                    self.db.cursor.execute(query, (id_tanaman_baru, id_jadwal))
                    self.db.connection.commit()
                    print("ID Tanaman berhasil diupdate!")
                    return True
                except ValueError:
                    print("Masukkan angka yang valid!")
            
            elif pilihan == "4":
                print("\nSTATUS JADWAL:")
                print("993 - Terjadwal")
                print("994 - Sedang Berlangsung") 
                print("995 - Siap Panen")
                
                try:
                    status_baru = int(input("ID Status baru: "))
                    if status_baru in [993, 994, 995]:
                        query = "UPDATE Jadwal_Tanam SET status_jadwal_id = %s WHERE id_jadwal_tanam = %s;"
                        self.db.cursor.execute(query, (status_baru, id_jadwal))
                        self.db.connection.commit()
                        print("Status berhasil diupdate!")
                        return True
                    else:
                        print("Status tidak valid!")
                except ValueError:
                    print("Masukkan angka yang valid!")
            
            else:
                print("Pilihan tidak valid!")
                
        except psycopg2.Error as e:
            print(f"Error mengedit jadwal: {e}")
            self.db.connection.rollback()
            return False
    
    def hapus_jadwal(self) -> bool:
        print("Fitur 1.4: Hapus Jadwal")
        try:
            print("\nHAPUS JADWAL TANAM")
            print("=" * 30)
            
            # Tampilkan jadwal yang ada
            jadwal_list = self.lihat_semua_jadwal()
            if not jadwal_list:
                return False
            
            while True:
                try:
                    id_jadwal = int(input("\nMasukkan ID Jadwal yang akan dihapus: "))
                    
                    # Cek apakah jadwal ada
                    query = "SELECT * FROM Jadwal_Tanam WHERE id_jadwal_tanam = %s;"
                    self.db.cursor.execute(query, (id_jadwal,))
                    jadwal = self.db.cursor.fetchone()
                    
                    if jadwal:
                        break
                    else:
                        print("ID jadwal tidak ditemukan!")
                except ValueError:
                    print("Masukkan angka yang valid!")
            
            # Konfirmasi penghapusan
            konfirmasi = input(f"\nYakin ingin menghapus jadwal ID {id_jadwal}? (y/n): ")
            
            if konfirmasi.lower() == 'y':
                # Cek apakah ada data terkait di tabel lain
                query_check = """
                SELECT COUNT(*) FROM Hasil_Panen WHERE id_jadwal_tanam = %s
                UNION ALL
                SELECT COUNT(*) FROM Masalah_Tanam WHERE id_jadwal_tanam = %s;
                """
                self.db.cursor.execute(query_check, (id_jadwal, id_jadwal))
                results = self.db.cursor.fetchall()
                
                total_terkait = sum([row[0] for row in results])
                
                if total_terkait > 0:
                    print(f"Jadwal ini memiliki {total_terkait} data terkait di tabel lain.")
                    konfirmasi_final = input("Tetap hapus? (y/n): ")
                    if konfirmasi_final.lower() != 'y':
                        print("Penghapusan dibatalkan")
                        return False
                
                # Hapus data terkait terlebih dahulu
                self.db.cursor.execute("DELETE FROM Hasil_Panen WHERE id_jadwal_tanam = %s;", (id_jadwal,))
                self.db.cursor.execute("DELETE FROM Masalah_Tanam WHERE id_jadwal_tanam = %s;", (id_jadwal,))
                
                # Hapus jadwal
                query = "DELETE FROM Jadwal_Tanam WHERE id_jadwal_tanam = %s;"
                self.db.cursor.execute(query, (id_jadwal,))
                self.db.connection.commit()
                
                print(f"Jadwal ID {id_jadwal} berhasil dihapus!")
                return True
            else:
                print("Penghapusan dibatalkan")
                return False
                
        except psycopg2.Error as e:
            print(f"Error menghapus jadwal: {e}")
            self.db.connection.rollback()
            return False
    
    def input_hasil_panen(self) -> bool:
        print("Fitur 1.5: Input Hasil Panen")
        try:
            print("\nINPUT HASIL PANEN")
            print("=" * 30)
            
            # Tampilkan jadwal yang berstatus "Siap Panen"
            query = """
            SELECT 
                jt.id_jadwal_tanam,
                t.nama_tanaman,
                jt.tanggal as tanggal_tanam,
                t.durasi_tanam,
                s.status
            FROM Jadwal_Tanam jt
            JOIN Tanaman t ON jt.id_tanaman = t.id_tanaman
            JOIN Status_Jadwal s ON jt.status_jadwal_id = s.id_status_jadwal
            WHERE jt.status_jadwal_id = 995
            ORDER BY jt.id_jadwal_tanam;
            """
            
            self.db.cursor.execute(query)
            jadwal_siap_panen = self.db.cursor.fetchall()
            
            if not jadwal_siap_panen:
                print("Tidak ada jadwal yang siap panen")
                return False
            
            print("\nJADWAL SIAP PANEN:")
            headers = ["ID Jadwal", "Tanaman", "Tanggal Tanam", "Durasi (hari)", "Status"]
            print(tabulate(jadwal_siap_panen, headers=headers, tablefmt="grid"))
            
            # Pilih jadwal
            while True:
                try:
                    id_jadwal = int(input("\nPilih ID Jadwal untuk input hasil panen: "))
                    jadwal_dipilih = next((j for j in jadwal_siap_panen if j[0] == id_jadwal), None)
                    if jadwal_dipilih:
                        break
                    else:
                        print("ID jadwal tidak valid atau tidak siap panen!")
                except ValueError:
                    print("Masukkan angka yang valid!")
            
            # Input data hasil panen
            print(f"\nInput hasil panen untuk jadwal ID: {id_jadwal}")
            
            while True:
                try:
                    tanggal_panen = input("Tanggal Panen (YYYY-MM-DD): ")
                    tanggal_obj = datetime.datetime.strptime(tanggal_panen, "%Y-%m-%d").date()
                    break
                except ValueError:
                    print("Format tanggal salah!")
            
            while True:
                try:
                    jumlah_panen = float(input("Jumlah Panen (kg): "))
                    if jumlah_panen > 0:
                        break
                    else:
                        print("Jumlah panen harus lebih dari 0!")
                except ValueError:
                    print("Masukkan angka yang valid!")
            
            while True:
                try:
                    harga_per_kg = float(input("Harga per kg (Rp): "))
                    if harga_per_kg > 0:
                        break
                    else:
                        print("Harga harus lebih dari 0!")
                except ValueError:
                    print("Masukkan angka yang valid!")
            
            # Ambil ID tanaman dari jadwal
            query_tanaman = "SELECT id_tanaman FROM Jadwal_Tanam WHERE id_jadwal_tanam = %s;"
            self.db.cursor.execute(query_tanaman, (id_jadwal,))
            id_tanaman = self.db.cursor.fetchone()[0]
            
            # Generate ID panen baru
            query_max_id = "SELECT COALESCE(MAX(id_panen), 5000) + 1 FROM Hasil_Panen;"
            self.db.cursor.execute(query_max_id)
            id_panen_baru = self.db.cursor.fetchone()[0]
            
            # Ambil id_kegiatan dari jadwal pemupukan (ambil yang terakhir untuk tanaman ini)
            query_kegiatan = """
            SELECT id_kegiatan FROM Jadwal_Pemupukan 
            WHERE id_tanaman = %s 
            ORDER BY id_kegiatan DESC LIMIT 1;
            """
            self.db.cursor.execute(query_kegiatan, (id_tanaman,))
            result_kegiatan = self.db.cursor.fetchone()
            id_kegiatan = result_kegiatan[0] if result_kegiatan else 301  # Default jika tidak ada
            
            # Insert hasil panen
            query_insert = """
            INSERT INTO Hasil_Panen (id_panen, tanggal, jumlah_panen_kg, harga_per_kg, id_tanaman, id_jadwal_tanam, id_kegiatan)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """
            
            self.db.cursor.execute(query_insert, (
                id_panen_baru, tanggal_obj, jumlah_panen, harga_per_kg, 
                id_tanaman, id_jadwal, id_kegiatan
            ))
            
            self.db.connection.commit()
            
            # Hitung total nilai panen
            total_nilai = jumlah_panen * harga_per_kg
            
            print(f"\nHasil panen berhasil dicatat!")
            print(f"ID Panen: {id_panen_baru}")
            print(f"Tanggal Panen: {tanggal_obj}")
            print(f"Jumlah Panen: {jumlah_panen} kg")
            print(f"Harga per kg: Rp {harga_per_kg:,.0f}")
            print(f"Total Nilai: Rp {total_nilai:,.0f}")
            
            return True
            
        except psycopg2.Error as e:
            print(f"Error input hasil panen: {e}")
            self.db.connection.rollback()
            return False

class Jadwal_Pemupukan:
    def __init__(self, conn, cur):
        self.conn = conn
        self.cur = cur

    def lihat_JadwalPemupukan(self):
        print("1: Lihat Jadwal Pemupukan")
        try:
            query = """
            SELECT 
                jp.id_kegiatan,
                t.id_tanaman || ' / ' || t.nama_tanaman AS tanaman,
                jp.nama_kegiatan,
                pp.jenis,
                jp.dosis_per_bibit_tanaman AS dosis_per_bibit,
                jp.tanggal_pemupukan AS waktu_pemupukan
            FROM Jadwal_Pemupukan jp
            JOIN Tanaman t ON jp.id_tanaman = t.id_tanaman
            JOIN Pupuk_Pestisida pp ON jp.id_kegiatan = pp.id_kegiatan
            ORDER BY jp.id_kegiatan;
            """
            self.cur.execute(query)
            results = self.cur.fetchall()
            
            if results:
                print("\nJADWAL PEMUPUKAN")
                print("=" * 80)
                columns = ["ID Kegiatan", "ID/Nama Tanaman", "Nama Kegiatan", "Jenis", "Waktu Pemupukan", "Dosis per Bibit"]
                print(tabulate([list(row) for row in results], headers=columns, tablefmt="grid"))
            else:
                print("Tidak ada jadwal pemupukan yang ditemukan")
        except psycopg2.Error as e:
            print(f"Error mengambil jadwal pemupukan: {e}")

    def tambah_JadwalPemupukan(self):
        print("2: Tambah Jadwal Pemupukan")
        try:
            print("\nTAMBAH JADWAL PEMUPUKAN")
            print("=" * 40)

            self.cur.execute("SELECT id_tanaman, nama_tanaman FROM Tanaman ORDER BY id_tanaman")
            tanaman_list = self.cur.fetchall()
            if not tanaman_list:
                print("Tidak ada data tanaman")
                return

            print("\nDAFTAR TANAMAN:")
            for t in tanaman_list:
                print(f"ID: {t['id_tanaman']} | Nama: {t['nama_tanaman']}")

            while True:
                try:
                    id_tanaman = int(input("\nPilih ID Tanaman: "))
                    tanaman_dipilih = next((t for t in tanaman_list if t['id_tanaman'] == id_tanaman), None)
                    if tanaman_dipilih:
                        break
                    else:
                        print("ID tanaman tidak valid!")
                except ValueError:
                    print("Masukkan angka yang valid!")

            while True:
                jenis = input("Jenis (Pupuk/Pestisida): ").strip().capitalize()
                if jenis in ["Pupuk", "Pestisida"]:
                    break
                print("Jenis harus 'Pupuk' atau 'Pestisida'! Coba lagi.")

            nama_kegiatan = input("Nama Kegiatan (misal: Pupuk awal, Pestisida): ").strip()
            nama_pupuk = input("Nama Pupuk/Pestisida (misal: Pupuk Organik, Pestisida): ").strip()
            while True:
                try:
                    interval_hari = int(input("Waktu Pemupukan (interval hari, misal: 5): "))
                    if interval_hari > 0:
                        break
                    else:
                        print("Interval hari harus lebih dari 0!")
                except ValueError:
                    print("Masukkan angka yang valid!")

            while True:
                try:
                    dosis_per_bibit = float(input("Dosis per Bibit (ml atau gr per tanaman): "))
                    if dosis_per_bibit > 0:
                        break
                    else:
                        print("Dosis harus lebih dari 0!")
                except ValueError:
                    print("Masukkan angka yang valid!")

            self.cur.execute("SELECT COALESCE(MAX(id_kegiatan), 300) + 1 FROM Jadwal_Pemupukan")
            id_kegiatan = self.cur.fetchone()[0]

            # Hitung tanggal pemupukan berdasarkan interval hari dari tanggal sekarang
            tanggal_pemupukan = datetime.now().date() + timedelta(days=interval_hari)
            self.cur.execute(
                "INSERT INTO Jadwal_Pemupukan (id_kegiatan, nama_kegiatan, tanggal_pemupukan, dosis_per_bibit_tanaman, id_tanaman) VALUES (%s, %s, %s, %s, %s)",
                (id_kegiatan, nama_kegiatan, tanggal_pemupukan, dosis_per_bibit, id_tanaman)
            )
            self.cur.execute(
                "INSERT INTO Pupuk_Pestisida (id_pupukpestisida, nama_barang, jenis, id_kegiatan) VALUES ((SELECT COALESCE(MAX(id_pupukpestisida), 200) + 1 FROM Pupuk_Pestisida), %s, %s, %s)",
                (nama_pupuk, jenis, id_kegiatan)
            )
            print(f"\nJadwal pemupukan berhasil ditambahkan! ID Kegiatan: {id_kegiatan}")
        except psycopg2.Error as e:
            print(f"Error menambah jadwal pemupukan: {e}")

    def ubah_JadwalPemupukan(self):
        print("3: Ubah Jadwal Pemupukan")
        try:
            print("\nUBAH JADWAL PEMUPUKAN")
            print("=" * 30)
            self.lihat_JadwalPemupukan()

            while True:
                try:
                    id_kegiatan = int(input("\nMasukkan ID Kegiatan yang akan diubah: "))
                    self.cur.execute("SELECT * FROM Jadwal_Pemupukan WHERE id_kegiatan = %s", (id_kegiatan,))
                    jadwal = self.cur.fetchone()
                    if jadwal:
                        break
                    else:
                        print("ID kegiatan tidak ditemukan!")
                except ValueError:
                    print("Masukkan angka yang valid!")

            print("\nPilih yang ingin diubah:")
            print("1. Tanggal Pemupukan")
            print("2. Dosis per Bibit")
            pilihan = input("Pilihan (1-2): ")

            if pilihan == "1":
                while True:
                    try:
                        tanggal_baru = input("Tanggal baru (YYYY-MM-DD): ")
                        tanggal_obj = datetime.strptime(tanggal_baru, "%Y-%m-%d").date()
                        break
                    except ValueError:
                        print("Format tanggal salah! Gunakan YYYY-MM-DD")
                self.cur.execute("UPDATE Jadwal_Pemupukan SET tanggal_pemupukan = %s WHERE id_kegiatan = %s", (tanggal_obj, id_kegiatan))
                print("Tanggal pemupukan berhasil diubah!")

            elif pilihan == "2":
                while True:
                    try:
                        dosis_baru = float(input("Dosis per Bibit baru (ml atau gr per tanaman): "))
                        if dosis_baru > 0:
                            break
                        else:
                            print("Dosis per bibit harus lebih dari 0!")
                    except ValueError:
                        print("Masukkan angka yang valid!")
                self.cur.execute("UPDATE Jadwal_Pemupukan SET dosis_per_bibit_tanaman = %s WHERE id_kegiatan = %s", (dosis_baru, id_kegiatan))
                print("Dosis per bibit berhasil diubah!")
        except psycopg2.Error as e:
            print(f"Error mengubah jadwal pemupukan: {e}")

    def hapus_JadwalPemupukan(self):
        print("4: Hapus Jadwal Pemupukan")
        try:
            print("\nHAPUS JADWAL PEMUPUKAN")
            print("=" * 30)
            self.cur.execute("""
                SELECT jp.id_kegiatan, t.id_tanaman || ' / ' || t.nama_tanaman AS tanaman, jp.nama_kegiatan, pp.jenis, jp.dosis_per_bibit_tanaman, jp.tanggal_pemupukan
                FROM Jadwal_Pemupukan jp
                JOIN Tanaman t ON jp.id_tanaman = t.id_tanaman
                JOIN Pupuk_Pestisida pp ON jp.id_kegiatan = pp.id_kegiatan
                ORDER BY jp.id_kegiatan;
            """)
            results = self.cur.fetchall()
            if results:
                print("\nJADWAL PEMUPUKAN")
                print("=" * 80)
                columns = ["ID Kegiatan", "ID/Nama Tanaman", "Nama Kegiatan", "Jenis", "Dosis per Bibit", "Tanggal Pemupukan"]
                print(tabulate([list(row) for row in results], headers=columns, tablefmt="grid"))
            else:
                print("Tidak ada jadwal pemupukan yang ditemukan")

            while True:
                try:
                    id_kegiatan = int(input("\nMasukkan ID Kegiatan yang akan dihapus: "))
                    self.cur.execute("SELECT * FROM Jadwal_Pemupukan WHERE id_kegiatan = %s", (id_kegiatan,))
                    jadwal = self.cur.fetchone()
                    if jadwal:
                        break
                    else:
                        print("ID kegiatan tidak ditemukan!")
                except ValueError:
                    print("Masukkan angka yang valid!")

            konfirmasi = input(f"Yakin ingin menghapus jadwal ID {id_kegiatan}? (ya/tidak): ")
            if konfirmasi.lower() == 'ya':
                self.cur.execute("DELETE FROM Pupuk_Pestisida WHERE id_kegiatan = %s", (id_kegiatan,))
                self.cur.execute("DELETE FROM Jadwal_Pemupukan WHERE id_kegiatan = %s", (id_kegiatan,))
                print(f"Jadwal pemupukan ID {id_kegiatan} berhasil dihapus!")
            else:
                print("Penghapusan dibatalkan")
        except psycopg2.Error as e:
            print(f"Error menghapus jadwal pemupukan: {e}")

    def lihatStok_pp(self):
        print("5: Lihat Stok Pupuk/Pestisida")
        try:
            self.cur.execute("SELECT * FROM Pupuk_Pestisida")
            results = self.cur.fetchall()
            if results:
                print("\nSTOK PUPUK/PESTISIDA")
                print("=" * 80)
                columns = ["ID", "Nama Barang", "Jenis", "ID Kegiatan"]
                print(tabulate([list(row) for row in results], headers=columns, tablefmt="grid"))
            else:
                print("Tidak ada stok pupuk/pestisida yang ditemukan")
        except psycopg2.Error as e:
            print(f"Error mengambil stok: {e}")

    def tambah_stok(self):
        print("6: Tambah Stok")
        try:
            print("\nTAMBAH STOK PUPUK/PESTISIDA")
            print("=" * 40)
            nama_barang = input("Nama Barang: ").strip()
            while True:
                jenis = input("Jenis (Pupuk/Pestisida): ").strip().capitalize()
                if jenis in ["Pupuk", "Pestisida"]:
                    break
                print("Jenis harus 'Pupuk' atau 'Pestisida'! Coba lagi.")
            self.cur.execute("SELECT id_kegiatan FROM Jadwal_Pemupukan")
            kegiatan_list = self.cur.fetchall()
            if not kegiatan_list:
                print("Tidak ada jadwal pemupukan yang tersedia untuk stok!")
                return
            print("\nDAFTAR ID KEGIATAN TERSEDIA:")
            for k in kegiatan_list:
                print(f"ID Kegiatan: {k['id_kegiatan']}")
            while True:
                try:
                    id_kegiatan = int(input("\nPilih ID Kegiatan untuk stok: "))
                    if any(k['id_kegiatan'] == id_kegiatan for k in kegiatan_list):
                        break
                    else:
                        print("ID kegiatan tidak valid!")
                except ValueError:
                    print("Masukkan angka yang valid!")
            self.cur.execute(
                "INSERT INTO Pupuk_Pestisida (id_pupukpestisida, nama_barang, jenis, id_kegiatan) VALUES ((SELECT COALESCE(MAX(id_pupukpestisida), 200) + 1 FROM Pupuk_Pestisida), %s, %s, %s)",
                (nama_barang, jenis, id_kegiatan)
            )
            print(f"Stok {nama_barang} berhasil ditambahkan!")
        except psycopg2.Error as e:
            print(f"Error menambah stok: {e}")

    def hapus_stok(self):
        print("7: Hapus Stok")
        try:
            print("\nHAPUS STOK PUPUK/PESTISIDA")
            print("=" * 30)
            self.lihatStok_pp()

            while True:
                try:
                    id_pupukpestisida = int(input("\nMasukkan ID Stok yang akan dihapus: "))
                    self.cur.execute("SELECT * FROM Pupuk_Pestisida WHERE id_pupukpestisida = %s", (id_pupukpestisida,))
                    stok = self.cur.fetchone()
                    if stok:
                        break
                    else:
                        print("ID stok tidak ditemukan!")
                except ValueError:
                    print("Masukkan angka yang valid!")

            konfirmasi = input(f"Yakin ingin menghapus stok ID {id_pupukpestisida}? (ya/tidak): ")
            if konfirmasi.lower() == 'ya':
                self.cur.execute("DELETE FROM Pupuk_Pestisida WHERE id_pupukpestisida = %s", (id_pupukpestisida,))
                print(f"Stok ID {id_pupukpestisida} berhasil dihapus!")
            else:
                print("Penghapusan dibatalkan")
        except psycopg2.Error as e:
            print(f"Error menghapus stok: {e}")

def clear_screen():
    print("Bersihkan layar konsol")
    os.system('cls' if os.name == 'nt' else 'clear')

def tampilkan_menu():
    print("Tampilkan menu utama")
    print("\n" + "="*50)
    print("SIPATANI - SISTEM INFORMASI PERTANIAN")
    print("FITUR 1: MANAJEMEN JADWAL TANAM")
    print("="*50)
    print("1.1 Lihat Semua Jadwal")
    print("1.2 Tambah Jadwal Baru")
    print("1.3 Edit Jadwal")
    print("1.4 Hapus Jadwal")
    print("1.5 Input Hasil Panen")
    print("1.6 Refresh Tampilan")
    print("0. Kembali ke Dashboard / Keluar")
    print("="*50)
    print("FITUR 2: MANAJEMEN JADWAL PEMUPUKAN DAN STOK")
    print("="*50)
    print("2.1 Lihat Jadwal Pemupukan")
    print("2.2 Tambah Jadwal Pemupukan")
    print("2.3 Ubah Jadwal Pemupukan")
    print("2.4 Hapus Jadwal Pemupukan")
    print("2.5 Lihat Stok Pupuk/Pestisida")
    print("2.6 Tambah Stok")
    print("2.7 Hapus Stok")
    print("="*50)

def main():
    print("Fungsi utama aplikasi")
    clear_screen()
    print("SIPATANI - Sistem Informasi Pertanian")
    print("Memulai aplikasi...")
    
    # Inisialisasi database
    conn, cur = connect()
    db = SipataniDatabase()
    if not db.connect():
        print("Gagal koneksi ke database. Program dihentikan.")
        return
    
    # Inisialisasi manager jadwal tanam
    jadwal_manager = JadwalTanamManager(db)
    manajemen_pemupukan = Jadwal_Pemupukan(conn, cur)
    
    try:
        while True:
            tampilkan_menu()
            menu_pemupukan()
            
            try:
                pilihan = input("\nPilih menu (0-6): ")
                
                if pilihan == "1.1":
                    clear_screen()
                    jadwal_manager.lihat_semua_jadwal()
                    input("\nTekan Enter untuk melanjutkan...")
                
                elif pilihan == "1.2":
                    clear_screen()
                    jadwal_manager.tambah_jadwal_baru()
                    input("\nTekan Enter untuk melanjutkan...")
                
                elif pilihan == "1.3":
                    clear_screen()
                    jadwal_manager.edit_jadwal()
                    input("\nTekan Enter untuk melanjutkan...")
                
                elif pilihan == "1.4":
                    clear_screen()
                    jadwal_manager.hapus_jadwal()
                    input("\nTekan Enter untuk melanjutkan...")
                
                elif pilihan == "1.5":
                    clear_screen()
                    jadwal_manager.input_hasil_panen()
                    input("\nTekan Enter untuk melanjutkan...")
                
                elif pilihan == "1.6":
                    clear_screen()
                    print("Tampilan di-refresh!")
                
                elif pilihan == "0":
                    clear_screen()
                    print("Terima kasih telah menggunakan SIPATANI!")
                    print("Menutup koneksi database...")
                    break

                elif pilihan == "2.1":
                clear_screen()
                manajemen_pemupukan.lihat_JadwalPemupukan()
    
                elif pilihan == "2.2":
                    clear_screen()
                    manajemen_pemupukan.tambah_JadwalPemupukan()
    
                elif pilihan == "2.3":
                    clear_screen()
                    manajemen_pemupukan.ubah_JadwalPemupukan()
    
                elif pilihan == "2.4":
                    clear_screen()
                    manajemen_pemupukan.hapus_JadwalPemupukan()
    
                elif pilihan == "2.5":
                    clear_screen()
                    manajemen_pemupukan.lihatStok_pp()
    
                elif pilihan == "2.6":
                    clear_screen()
                    manajemen_pemupukan.tambah_stok()
    
                elif pilihan == "2.7":
                    clear_screen()
                    manajemen_pemupukan.hapus_stok()
    
                else:
                    print("Pilihan tidak valid! Silakan pilih dengan benar!")
                    input("\nTekan Enter untuk melanjutkan...")
                    
            except KeyboardInterrupt:
                print("\n\nProgram dihentikan oleh user")
                break
            except Exception as e:
                print(f"\nError tidak terduga: {e}")
                input("\nTekan Enter untuk melanjutkan...")
    
    finally:
        db.disconnect()

if __name__ == "__main__":
    # Cek apakah library yang diperlukan sudah terinstall
    try:
        import psycopg2
        from tabulate import tabulate
    except ImportError as e:
        print(f"Library yang diperlukan belum terinstall: {e}")
        print("Jalankan perintah berikut untuk menginstall:")
        print("pip install psycopg2-binary tabulate")
        exit(1)
    
    main()
class LaporanMasalahDB:
    def __init__(self, dbname, user, password, host='localhost', port=5432):
        self.connection_config = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.connection_config)
            print("Berhasil terhubung ke database PostgreSQL.")
            self.create_table_if_not_exists()
        except Exception as e:
            print(f"Error koneksi ke database: {e}")
            raise e

    def close(self):
        if self.conn:
            self.conn.close()
            print("Koneksi database ditutup.")

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS laporan_masalah (
            id SERIAL PRIMARY KEY,
            id_jadwal_tanam INTEGER NOT NULL,
            tanggal_masalah DATE NOT NULL,
            jenis VARCHAR(50) NOT NULL,
            deskripsi TEXT NOT NULL,
            status_penanganan VARCHAR(20) NOT NULL CHECK (status_penanganan IN ('Belum', 'Proses', 'Selesai')),
            solusi TEXT
        );
        """
        with self.conn.cursor() as cur:
            cur.execute(create_table_query)
            self.conn.commit()
            print("Tabel 'laporan_masalah' siap digunakan.")

    def lihat_laporan(self):
        query = "SELECT * FROM laporan_masalah ORDER BY id;"
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            records = cur.fetchall()
            if not records:
                print("Belum ada laporan masalah.")
            else:
                print("Daftar Laporan Masalah:")
                for row in records:
                    print(f"ID Laporan     : {row['id']}")
                    print(f"ID Jadwal Tanam: {row['id_jadwal_tanam']}")
                    print(f"Tanggal Masalah: {row['tanggal_masalah']}")
                    print(f"Jenis Masalah  : {row['jenis']}")
                    print(f"Deskripsi      : {row['deskripsi']}")
                    print(f"Status         : {row['status_penanganan']}")
                    print(f"Solusi         : {row['solusi'] if row['solusi'] else '-'}")
                    print("-" * 40)

    def tambah_laporan(self, id_jadwal_tanam, tanggal_masalah, jenis, deskripsi, status_penanganan, solusi):
        insert_query = """
        INSERT INTO laporan_masalah
        (id_jadwal_tanam, tanggal_masalah, jenis, deskripsi, status_penanganan, solusi)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        try:
            tanggal_obj = datetime.strptime(tanggal_masalah, "%Y-%m-%d").date()
        except ValueError:
            print("Format tanggal salah. Gunakan format YYYY-MM-DD.")
            return
        if status_penanganan not in ('Belum', 'Proses', 'Selesai'):
            print("Status penanganan harus salah satu dari: Belum, Proses, Selesai")
            return
        with self.conn.cursor() as cur:
            cur.execute(insert_query, (id_jadwal_tanam, tanggal_obj, jenis, deskripsi, status_penanganan, solusi))
            new_id = cur.fetchone()[0]
            self.conn.commit()
            print(f"Laporan baru berhasil ditambahkan dengan ID {new_id}.")

    def edit_laporan(self, id_laporan, id_jadwal_tanam, tanggal_masalah, jenis, deskripsi, status_penanganan, solusi):
        update_query = """
        UPDATE laporan_masalah 
        SET id_jadwal_tanam=%s, tanggal_masalah=%s, jenis=%s, deskripsi=%s, status_penanganan=%s, solusi=%s
        WHERE id=%s;
        """
        try:
            tanggal_obj = datetime.strptime(tanggal_masalah, "%Y-%m-%d").date()
        except ValueError:
            print("Format tanggal salah. Gunakan format YYYY-MM-DD.")
            return
        if status_penanganan not in ('Belum', 'Proses', 'Selesai'):
            print("Status penanganan harus salah satu dari: Belum, Proses, Selesai")
            return
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM laporan_masalah WHERE id=%s;", (id_laporan,))
            if cur.fetchone() is None:
                print(f"Laporan dengan ID {id_laporan} tidak ditemukan.")
                return
            cur.execute(update_query, (id_jadwal_tanam, tanggal_obj, jenis, deskripsi, status_penanganan, solusi, id_laporan))
            self.conn.commit()
            print(f"Laporan dengan ID {id_laporan} berhasil diedit.")

    def hapus_laporan(self, id_laporan):
        delete_query = "DELETE FROM laporan_masalah WHERE id=%s;"
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM laporan_masalah WHERE id=%s;", (id_laporan,))
            if cur.fetchone() is None:
                print(f"Laporan dengan ID {id_laporan} tidak ditemukan.")
                return
            cur.execute(delete_query, (id_laporan,))
            self.conn.commit()
            print(f"Laporan dengan ID {id_laporan} berhasil dihapus.")

    def update_status_solusi(self, id_laporan, status_penanganan, solusi):
        if status_penanganan not in ('Belum', 'Proses', 'Selesai'):
            print("Status penanganan harus salah satu dari: Belum, Proses, Selesai")
            return
        update_query = """
        UPDATE laporan_masalah 
        SET status_penanganan=%s, solusi=%s
        WHERE id=%s;
        """
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM laporan_masalah WHERE id=%s;", (id_laporan,))
            if cur.fetchone() is None:
                print(f"Laporan dengan ID {id_laporan} tidak ditemukan.")
                return
            cur.execute(update_query, (status_penanganan, solusi, id_laporan))
            self.conn.commit()
            print(f"Status penanganan dan solusi laporan ID {id_laporan} berhasil diperbarui.")

    def kembali_ke_dashboard(self):
        print("Kembali ke Dashboard.")
