INSERT INTO STORAGE_DATABASE.CPW_DATA.MASTER_ANGLER_AWARD (
	"angler",
	"species",
	"length",
	"location",
	"date",
	"released"
)
SELECT
    "Angler",
	"Species",
	"Length",
	"Location",
	"Date",
	"Released"
from STORAGE_DATABASE.CPW_DATA.MASTER_ANGLER_AWARD_TEMP