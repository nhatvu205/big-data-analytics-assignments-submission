# Lab 1 - Big Data Analytics
*Sinh viên thực hiện: 23521104
Họ và tên: Vũ Đình Nhật*

## Cấu trúc thư mục

```text
lab1/
├── assignments.ipynb              # Đề bài
├── README.md                      # Mô tả folder
├── src/                           # Mã nguồn Java MapReduce
│   ├── Bai1DiemTBPhim.java
│   ├── Bai2DiemTBTheLoai.java
│   ├── Bai3DiemTheoGioiTinh.java
│   └── Bai4DiemTheoNhomTuoi.java
├── bin/                           # Thư mục build .class (sinh khi compile)
├── Bai1DiemTBPhim.jar             # Jar bài 1
├── Bai2DiemTBTheLoai.jar          # Jar bài 2
├── Bai3DiemTheoGioiTinh.jar       # Jar bài 3
├── Bai4DiemTheoNhomTuoi.jar       # Jar bài 4
├── input/                         # Dữ liệu đầu vào được cung cấp
│   ├── movies.txt
│   ├── ratings_1.txt
│   ├── ratings_2.txt
│   └── users.txt
└── output/                        # Kết quả kéo về local từ HDFS
    ├── bai1.txt
    ├── bai2.txt
    ├── bai3.txt
    └── bai4.txt
```

## Minh chứng kết quả

![Kết quả bài 1](https://private-user-images.githubusercontent.com/146966505/571888188-66f22145-7327-4347-bcbb-1438f74b47a7.png?jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NzQ5NjA0ODEsIm5iZiI6MTc3NDk2MDE4MSwicGF0aCI6Ii8xNDY5NjY1MDUvNTcxODg4MTg4LTY2ZjIyMTQ1LTczMjctNDM0Ny1iY2JiLTE0MzhmNzRiNDdhNy5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjYwMzMxJTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI2MDMzMVQxMjI5NDFaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT1lNGFhOWVmNjdiNzdjYzlhNDNkNmJhZDljMDlmMWJmOWRiMTIzYjQyN2ZjZjVmNTM1Y2RkNzUzYzBkNWM2NGQzJlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9.diDBZipq2eDpYyRTTaSpAkuDtZdjuoBx422PuJT_7q8)