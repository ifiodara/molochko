create table dev_supply.manufactures
(
	manufacture_id int PRIMARY KEY GENERATED BY DEFAULT AS identity,
	name_part varchar(50),
	manufacture varchar(50),
	extract_category varchar(40)
)
;