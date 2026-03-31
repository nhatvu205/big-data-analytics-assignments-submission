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

![Kết quả bài 1](https://private-user-images.githubusercontent.com/146966505/571888188-66f22145-7327-4347-bcbb-1438f74b47a7.png?jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NzQ5NjA0ODEsIm5iZiI6MTc3NDk2MDE4MSwicGF0aCI6Ii8xNDY5NjY1MDUvNTcxODg4MTg4LTY2ZjIyMTQ1LTczMjctNDM0Ny1iY2JiLTE0MzhmNzRiNDdhNy5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjYwMzMxJTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI2MDMzMVQxMjI5NDFaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT1lNGFhOWVmNjdiNzdjYzlhNDNkNmJhZDljMDlmMWJmOWRiMTIzYjQyN2ZjZjVmNTM1Y2RkNzUzYzBkNWM2NGQzJlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9.diDBZipq2eDpYyRTTaSpAkuDtZdjuoBx422PuJT_7q8 "Kết quả bài 1")

![Kết quả bài 2](https://private-user-images.githubusercontent.com/146966505/571888187-278f5637-3e2f-41b4-be56-62166e10e696.png?jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NzQ5NjA1NzQsIm5iZiI6MTc3NDk2MDI3NCwicGF0aCI6Ii8xNDY5NjY1MDUvNTcxODg4MTg3LTI3OGY1NjM3LTNlMmYtNDFiNC1iZTU2LTYyMTY2ZTEwZTY5Ni5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjYwMzMxJTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI2MDMzMVQxMjMxMTRaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT0wMTI2YzcyYTFlZTQ0ZjhmMThlZGY3N2FhMzkzODJhN2I2MzZkMzA4Njk4ZmYxZTM4NDJkOGMwOTRlOWVmMTg2JlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9.mL8jrL7npkUJBSz4Rk8r7yQK7gOJiFafn7szQQcbXiY "Kết quả bài 2")

![Kết quả bài 3](https://private-user-images.githubusercontent.com/146966505/571888189-7f3ad02b-ff77-43c4-8c2f-d1b0f1efc308.png?jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NzQ5NjA1NzQsIm5iZiI6MTc3NDk2MDI3NCwicGF0aCI6Ii8xNDY5NjY1MDUvNTcxODg4MTg5LTdmM2FkMDJiLWZmNzctNDNjNC04YzJmLWQxYjBmMWVmYzMwOC5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjYwMzMxJTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI2MDMzMVQxMjMxMTRaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT1mNTViOGZhMTk0NDUxMmEyYjMzZGI3ODYwM2FhZThlOTU2NmVkNTViNmEzY2Q2ZGMwYTNhNzAwMTE3YzI4ZDBhJlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9.oVQ6XzwtnYXRYggz3gXe5xyqqHZ3815lVy45VVFKGA8 "Kết quả bài 3")

![Kết quả bài 4](https://private-user-images.githubusercontent.com/146966505/571888186-4136bbda-c6ba-40ce-8bf6-7986dfdf3a80.png?jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NzQ5NjA1NzQsIm5iZiI6MTc3NDk2MDI3NCwicGF0aCI6Ii8xNDY5NjY1MDUvNTcxODg4MTg2LTQxMzZiYmRhLWM2YmEtNDBjZS04YmY2LTc5ODZkZmRmM2E4MC5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjYwMzMxJTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI2MDMzMVQxMjMxMTRaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT1kYTBlZTk2NGNlMGUyZmVlNzI2ZDAyYTRmNWViNTJlMmZjNjE1YjVkMjFhOTAyZGI4ZDFiMTNiNWJjODcxMDU5JlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9.Ce30wvjP1qHkQgHNe0SeBA42uuUnuhRmqU-xqDPJZjc "Kết quả bài 4")