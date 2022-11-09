BEGIN;

SET client_encoding = 'LATIN1';

CREATE SCHEMA prod;

-- Here create the table for the ETL output
CREATE TABLE prod.most_rel_microw (
	user_id serial4 NOT NULL,
	id varchar(20) NOT NULL,
	site_id varchar(5) NOT NULL,
	title varchar(355) NOT NULL,
	price float8 NULL,
	sold_quantity int4 NULL,
	thumbnail varchar(355) NULL,
	created_date timestamp NOT NULL,
	CONSTRAINT most_rel_microw_pkey PRIMARY KEY (user_id)
);

COMMIT;