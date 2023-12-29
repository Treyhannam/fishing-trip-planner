INSERT INTO STORAGE_DATABASE.CPW_DATA.MASTER_ANGLER_AWARD (
	"angler",
	"species",
	"length",
	"location",
	"date",
	"released"
)
SELECT
    "angler",
	"species",
	"length",
	"location",
	"date",
	"released"
from STORAGE_DATABASE.CPW_DATA.MASTER_ANGLER_AWARD_TEMP