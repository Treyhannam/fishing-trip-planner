create table if not exists STORAGE_DATABASE.CPW_DATA.STORAGE_DATABASE.CPW_DATA.ALL_SPECIES (
    all_species_id NUMBER(38,0) autoincrement start 0 increment by 1,
	"angler" VARCHAR(1000),
	"species" VARCHAR(100),
	"length" NUMBER(2, 0),
	"location" VARCHAR(100),
	"date" VARCHAR(15),
	"released" VARCHAR(3),
);