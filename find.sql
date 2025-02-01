WITH target_ids AS (
    SELECT id FROM word_mapping WHERE word IN ('apple', 'apples')
),
target_rows AS (
    SELECT rowid AS row_num, * FROM words WHERE word_id IN (SELECT id FROM target_ids)
)
SELECT wm.word
FROM words w
JOIN target_rows t ON w.rowid BETWEEN t.row_num - 8 AND t.row_num + 8
JOIN word_mapping wm ON w.word_id = wm.id
ORDER BY w.rowid;
-- 没有给words表建索引的话大概1分钟到2分钟，建索引后应该几秒
