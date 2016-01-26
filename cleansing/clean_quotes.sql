UPDATE movie
SET title = SUBSTR(title,2)
WHERE title LIKE '"%' OR title LIKE '''%';

UPDATE movie
SET original_title = SUBSTR(original_title,2)
WHERE original_title LIKE '"%' OR original_title LIKE '''%';

UPDATE person 
SET last_name = SUBSTR(last_name,2)
WHERE last_name LIKE '"%' OR last_name LIKE '''%';

UPDATE person
SET first_name = SUBSTR(first_name,2)
WHERE first_name LIKE '"%' OR first_name LIKE '''%';

UPDATE person
SET stage_name = SUBSTR(stage_name,2)
WHERE stage_name LIKE '"%' OR stage_name LIKE '''%';

UPDATE work
SET title = SUBSTR(title,2)
WHERE title LIKE '"%' OR title LIKE '''%';
