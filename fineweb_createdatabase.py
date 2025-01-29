import time

from datatrove.pipeline.readers import ParquetReader
import nltk
from nltk.tokenize import word_tokenize
import sqlite3

# 初始化数据库
token_db_path = "./tokens_fineweb.db"
conn = sqlite3.connect(token_db_path)
cursor = conn.cursor()

# 创建单词表（存储唯一单词及其ID）
cursor.execute("CREATE TABLE IF NOT EXISTS word_mapping (id INTEGER PRIMARY KEY, word TEXT UNIQUE)")
# 创建词频表（存储单词ID）
cursor.execute("CREATE TABLE IF NOT EXISTS words (word_id INTEGER, FOREIGN KEY(word_id) REFERENCES word_mapping(id))")
conn.commit()

# 清空表
# cursor.execute("DELETE FROM words")
# cursor.execute("DELETE FROM word_mapping")
conn.commit()

# 读取数据
data_reader = ParquetReader("hf://datasets/HuggingFaceFW/fineweb/data/CC-MAIN-2024-51", limit=1500000, batch_size=3000, skip=0) # limit 1000 000 数据库7GB 630 000 000个单词 大概2小时爬完
# 加上skip 还是要下载，只是下载完会跳过

skip_count = 0
# 处理文档并存入数据库
for document in data_reader():
    print(document.id)
    skip_count += 1
    print(skip_count, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    sentences = document.text.split('\n')

    for sentence in sentences:
        words = word_tokenize(sentence)  # 分词处理
        for word in words:
            cursor.execute("INSERT OR IGNORE INTO word_mapping (word) VALUES (?)", (word,))
            cursor.execute("INSERT INTO words (word_id) VALUES ((SELECT id FROM word_mapping WHERE word = ?))", (word,))
    if skip_count % 5000 == 0:
        conn.commit() # 不commit python网络异常中断，会恢复原本
        print('commit')


conn.close()
