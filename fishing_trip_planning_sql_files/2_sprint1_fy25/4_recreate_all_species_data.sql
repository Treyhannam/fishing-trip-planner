create table if not exists STORAGE_DATABASE.CPW_DATA.ALL_SPECIES (
    all_species_id NUMBER(38,0) autoincrement start 0 increment by 1,
	"main_species" varchar(100),
	"fish_species" varchar(100),
	"water" varchar(100),
	"county" varchar(100),
	"property_name" varchar(100),
	"ease_of_access" varchar(100),
	"boating" varchar(100),
	"fishing_pressure" varchar(100),
	"stocked" varchar(100),
	"elevation(ft)" varchar(100),
	"latitude" varchar(100),
	"longitude" varchar(100)
);