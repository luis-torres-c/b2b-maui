CREATE TABLE IF NOT EXISTS tracker(
	name VARCHAR(50),
	last_processed_day INTEGER,
	last_flag INTEGER
);

CREATE TABLE IF NOT EXISTS locker(
	name VARCHAR(50),
	is_locked INTEGER
);

CREATE TABLE IF NOT EXISTS counter(
  name VARCHAR(50),
  count INTEGER
);

CREATE TABLE IF NOT EXISTS processedfiles (
	file VARCHAR(255),
	checksum CHAR(32),
	name VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS keysvalues (
	name VARCHAR(50),
	json TEXT
);

CREATE TABLE IF NOT EXISTS periods (
	tag VARCHAR(64),
	date_to VARCHAR(10),
	date_from VARCHAR(10),
	status VARCHAR(15)
);
