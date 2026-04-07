raw = LOAD '/lab2/input/hotel-review.csv' USING PigStorage(';')
      AS (id:int, comment:chararray, category:chararray, aspect:chararray, sentiment:chararray);

-- Bài 1
-- Đọc stopwords và chuẩn hóa về chữ thường
sw = LOAD '/lab2/input/stopwords.txt' AS (w:chararray);
sw1 = FOREACH sw GENERATE LOWER(TRIM(w)) AS w;

-- Tách từ từ comment + chuẩn hóa sentiment
tok = FOREACH raw GENERATE
      id, category, aspect, LOWER(sentiment) AS sentiment,
      FLATTEN(TOKENIZE(LOWER(comment))) AS word;
tok2 = FOREACH tok GENERATE id, category, aspect, sentiment,
       TRIM(REPLACE(word, '[^\\p{L}\\p{N}]', '')) AS word;
tok3 = FILTER tok2 BY word IS NOT NULL AND word != '';

-- Bỏ stopwords
j = JOIN tok3 BY word LEFT OUTER, sw1 BY w;
clean = FILTER j BY sw1::w IS NULL;
words = FOREACH clean GENERATE tok3::id AS id, tok3::category AS category,
        tok3::aspect AS aspect, tok3::sentiment AS sentiment, tok3::word AS word;
STORE words INTO '/lab2/output/bai1/clean_words';

-- Bài 2
-- Đếm từ xuất hiện trên 500 lần
g1 = GROUP words BY word;
word_freq = FOREACH g1 GENERATE group AS word, COUNT(words) AS cnt;
word_500 = FILTER word_freq BY cnt > 500;
STORE word_500 INTO '/lab2/output/bai2/word_over_500';

-- Thống kê số bình luận theo category
cat_pairs = DISTINCT (FOREACH raw GENERATE id, category);
g2 = GROUP cat_pairs BY category;
cat_count = FOREACH g2 GENERATE group AS category, COUNT(cat_pairs) AS cnt;
STORE cat_count INTO '/lab2/output/bai2/count_by_cat';

-- Thống kê số bình luận theo aspect
asp_pairs = DISTINCT (FOREACH raw GENERATE id, aspect);
g3 = GROUP asp_pairs BY aspect;
asp_count = FOREACH g3 GENERATE group AS aspect, COUNT(asp_pairs) AS cnt;
STORE asp_count INTO '/lab2/output/bai2/count_by_asp';

-- Bài 3

-- Aspect có negative comment nhiều nhất
neg = FILTER raw BY LOWER(sentiment) == 'negative';
neg_pairs = DISTINCT (FOREACH neg GENERATE id, aspect);
gneg = GROUP neg_pairs BY aspect;
neg_count = FOREACH gneg GENERATE group AS aspect, COUNT(neg_pairs) AS cnt;
neg_all = GROUP neg_count ALL;
neg_max = FOREACH neg_all GENERATE MAX(neg_count.cnt) AS max_cnt;
neg_top_join = JOIN neg_count BY cnt, neg_max BY max_cnt;
neg_top = FOREACH neg_top_join GENERATE neg_count::aspect AS aspect, neg_count::cnt AS cnt;
STORE neg_top INTO '/lab2/output/bai3/top_neg_asp';

-- Aspect có positive comment nhiều nhất
pos = FILTER raw BY LOWER(sentiment) == 'positive';
pos_pairs = DISTINCT (FOREACH pos GENERATE id, aspect);
gpos = GROUP pos_pairs BY aspect;
pos_count = FOREACH gpos GENERATE group AS aspect, COUNT(pos_pairs) AS cnt;
pos_all = GROUP pos_count ALL;
pos_max = FOREACH pos_all GENERATE MAX(pos_count.cnt) AS max_cnt;
pos_top_join = JOIN pos_count BY cnt, pos_max BY max_cnt;
pos_top = FOREACH pos_top_join GENERATE pos_count::aspect AS aspect, pos_count::cnt AS cnt;
STORE pos_top INTO '/lab2/output/bai3/top_pos_asp';

-- Bài 4
-- Top 5 từ positive theo category
pos_w = FILTER words BY sentiment == 'positive';
g4p = GROUP pos_w BY (category, word);
pos_wc = FOREACH g4p GENERATE group.category AS category, group.word AS word, COUNT(pos_w) AS cnt;
grp_p = GROUP pos_wc BY category;
top5_pos = FOREACH grp_p { s = ORDER pos_wc BY cnt DESC; t = LIMIT s 5; GENERATE FLATTEN(t); };
STORE top5_pos INTO '/lab2/output/bai4/top5_pos_words_by_cat';

-- Top 5 từ negative theo category
neg_w = FILTER words BY sentiment == 'negative';
g4n = GROUP neg_w BY (category, word);
neg_wc = FOREACH g4n GENERATE group.category AS category, group.word AS word, COUNT(neg_w) AS cnt;
grp_n = GROUP neg_wc BY category;
top5_neg = FOREACH grp_n { s = ORDER neg_wc BY cnt DESC; t = LIMIT s 5; GENERATE FLATTEN(t); };
STORE top5_neg INTO '/lab2/output/bai4/top5_neg_words_by_cat';

-- Bài 5
-- Top 5 từ liên quan nhất theo category (dựa trên tần số)
g5 = GROUP words BY (category, word);
rel_wc = FOREACH g5 GENERATE group.category AS category, group.word AS word, COUNT(words) AS cnt;
grp5 = GROUP rel_wc BY category;
top5_rel = FOREACH grp5 { s = ORDER rel_wc BY cnt DESC; t = LIMIT s 5; GENERATE FLATTEN(t); };
STORE top5_rel INTO '/lab2/output/bai5/top5_rel_by_cat';
