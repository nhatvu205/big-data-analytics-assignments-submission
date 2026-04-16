from datetime import datetime
from pathlib import Path

from pyspark import SparkContext


BASE_DIR = Path(__file__).resolve().parent
MIN_RATING_COUNT = 5


def parse_movie(line):
    movie_id, rest = line.split(",", 1)
    title, genres = rest.rsplit(",", 1)
    return movie_id, (title, genres.split("|"))


def parse_rating(line):
    user_id, movie_id, rating, timestamp = line.split(",")
    return user_id, movie_id, float(rating), int(timestamp)


def parse_user(line):
    user_id, gender, age, occupation_id, _ = line.split(",")
    return user_id, (gender, int(age), occupation_id)


def parse_occupation(line):
    occupation_id, occupation_name = line.split(",", 1)
    return occupation_id, occupation_name


def to_stat(rating):
    return rating, 1


def merge_stat(left, right):
    return left[0] + right[0], left[1] + right[1]


def finalize_stat(item):
    key, (total_rating, total_count) = item
    return key, (round(total_rating / total_count, 2), total_count)


def age_group(age):
    if age < 18:
        return "<18"
    if age <= 24:
        return "18-24"
    if age <= 34:
        return "25-34"
    if age <= 44:
        return "35-44"
    if age <= 54:
        return "45-54"
    return "55+"


def print_section(title):
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


def print_rows(rows, formatter):
    for row in rows:
        print(formatter(row))


def main():
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("ERROR")

    # Reuse các RDD gốc vì 6 bài cùng đọc chung dữ liệu
    movies = sc.textFile(str(BASE_DIR / "movies.txt")).map(parse_movie).cache()
    ratings = (
        sc.textFile(str(BASE_DIR / "ratings_1.txt"))
        .union(sc.textFile(str(BASE_DIR / "ratings_2.txt")))
        .map(parse_rating)
        .cache()
    )
    users = sc.textFile(str(BASE_DIR / "users.txt")).map(parse_user).cache()
    occupations = sc.textFile(str(BASE_DIR / "occupation.txt")).map(parse_occupation).cache()

    movie_titles = movies.mapValues(lambda value: value[0]).cache()
    movie_genres = movies.mapValues(lambda value: value[1]).cache()

    # Bài 1: trung bình và số lượt đánh giá mỗi phim
    movie_stats = (
        ratings.map(lambda row: (row[1], to_stat(row[2])))
        .reduceByKey(merge_stat)
        .map(finalize_stat)
    )
    movie_results = (
        movie_stats.join(movie_titles)
        .map(lambda row: (row[0], row[1][1], row[1][0][0], row[1][0][1]))
        .sortBy(lambda row: (-row[2], -row[3], row[1]))
        .collect()
    )

    print_section("BAI 1 - DIEM TRUNG BINH VA SO LUOT DANH GIA CUA MOI PHIM")
    print_rows(
        movie_results,
        lambda row: f"{row[0]} | {row[1]} | avg={row[2]:.2f} | count={row[3]}",
    )

    top_movie = next((row for row in movie_results if row[3] >= MIN_RATING_COUNT), None)
    print("\nPhim co diem trung binh cao nhat va it nhat 5 luot danh gia:")
    if top_movie:
        print(
            f"{top_movie[0]} | {top_movie[1]} | avg={top_movie[2]:.2f} | count={top_movie[3]}"
        )
    else:
        print("Khong co phim nao dat nguong 5 luot danh gia.")

    # Bài 2: Phân tích đánh giá theo thể loại
    genre_results = (
        ratings.map(lambda row: (row[1], row[2]))
        .join(movie_genres)
        .flatMap(lambda row: ((genre, to_stat(row[1][0])) for genre in row[1][1]))
        .reduceByKey(merge_stat)
        .map(finalize_stat)
        .map(lambda row: (row[0], row[1][0], row[1][1]))
        .sortBy(lambda row: (-row[1], -row[2], row[0]))
        .collect()
    )

    print_section("BAI 2 - DIEM TRUNG BINH THEO THE LOAI")
    print_rows(
        genre_results,
        lambda row: f"{row[0]} | avg={row[1]:.2f} | count={row[2]}",
    )

    user_gender = users.mapValues(lambda value: value[0]).cache()
    gender_results = (
        ratings.map(lambda row: (row[0], (row[1], row[2])))
        .join(user_gender)
        .map(lambda row: ((row[1][0][0], row[1][1]), to_stat(row[1][0][1])))
        .reduceByKey(merge_stat)
        .map(finalize_stat)
        # Dua ve dung dang (movie_id, (gender, avg, count)) truoc khi join voi title
        .map(lambda row: (row[0][0], (row[0][1], row[1][0], row[1][1])))
        .join(movie_titles)
        .map(lambda row: (row[0], row[1][1], row[1][0][0], row[1][0][1], row[1][0][2]))
        .sortBy(lambda row: (row[1], row[0]))
        .collect()
    )
    # Bài 3: Phân tích đánh giá theo giới tính
    print_section("BAI 3 - DIEM TRUNG BINH MOI PHIM THEO GIOI TINH")
    print_rows(
        gender_results,
        lambda row: f"{row[0]} | {row[1]} | gender={row[2]} | avg={row[3]:.2f} | count={row[4]}",
    )

    user_age_group = users.mapValues(lambda value: age_group(value[1])).cache()
    age_results = (
        ratings.map(lambda row: (row[0], (row[1], row[2])))
        .join(user_age_group)
        .map(lambda row: ((row[1][0][0], row[1][1]), to_stat(row[1][0][1])))
        .reduceByKey(merge_stat)
        .map(finalize_stat)
        # Dua ve dung dang (movie_id, (age_group, avg, count)) truoc khi join voi title
        .map(lambda row: (row[0][0], (row[0][1], row[1][0], row[1][1])))
        .join(movie_titles)
        .map(lambda row: (row[0], row[1][1], row[1][0][0], row[1][0][1], row[1][0][2]))
        .sortBy(lambda row: (row[1], row[0]))
        .collect()
    )
    # Bài 4: Phân tích đánh giá theo nhóm tuổi
    print_section("BAI 4 - DIEM TRUNG BINH MOI PHIM THEO NHOM TUOI")
    print_rows(
        age_results,
        lambda row: f"{row[0]} | {row[1]} | age_group={row[2]} | avg={row[3]:.2f} | count={row[4]}",
    )

    occupation_lookup = users.map(lambda row: (row[1][2], row[0])).join(occupations).map(
        lambda row: (row[1][0], row[1][1])
    )
    occupation_results = (
        ratings.map(lambda row: (row[0], row[2]))
        .join(occupation_lookup)
        .map(lambda row: (row[1][1], to_stat(row[1][0])))
        .reduceByKey(merge_stat)
        .map(finalize_stat)
        .map(lambda row: (row[0], row[1][0], row[1][1]))
        .sortBy(lambda row: (-row[1], -row[2], row[0]))
        .collect()
    )
    # Bài 5: Phân tích đánh giá theo nghề nghiệp
    print_section("BAI 5 - DIEM TRUNG BINH THEO NGHE NGHIEP")
    print_rows(
        occupation_results,
        lambda row: f"{row[0]} | avg={row[1]:.2f} | count={row[2]}",
    )

    time_results = (
        ratings.map(
            lambda row: (datetime.utcfromtimestamp(row[3]).year, to_stat(row[2]))
        )
        .reduceByKey(merge_stat)
        .map(finalize_stat)
        .map(lambda row: (row[0], row[1][0], row[1][1]))
        .sortBy(lambda row: row[0])
        .collect()
    )
    # Bài 6: Phân tích đánh giá theo năm
    print_section("BAI 6 - DIEM TRUNG BINH VA SO LUOT DANH GIA THEO NAM")
    print_rows(
        time_results,
        lambda row: f"{row[0]} | avg={row[1]:.2f} | count={row[2]}",
    )

    sc.stop()


if __name__ == "__main__":
    main()
