CREATE TABLE e_movie (
	tagline		TEXT,
	title		TEXT,
	release_year		INTEGER
);

CREATE TABLE e_person (
	year_of_birth		INTEGER,
	name		TEXT
);

CREATE TABLE r_acted_in (
	from_id		INTEGER,
	to_id		INTEGER,
	roles		TEXT ARRAY
);

CREATE TABLE r_directed (
	from_id		INTEGER,
	to_id		INTEGER
);

CREATE TABLE r_produced (
	from_id		INTEGER,
	to_id		INTEGER
);

CREATE TABLE r_wrote (
	from_id		INTEGER,
	to_id		INTEGER
);

CREATE TABLE r_reviewed (
	from_id		INTEGER,
	to_id		INTEGER,
	review_text		TEXT,
	rating		NUMERIC
);