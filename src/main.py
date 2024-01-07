from web_scrapers import master_angler, colorado_fishing_atlas
from data_processing import clean_master_angler_data, clean_atlas_data
from snowflake_ import snowflake_writer

raw_data = master_angler.MasterAnglerScraper().execute()

df = clean_master_angler_data.process_master_angler_data(raw_data)

snowflake_writer.SnowflakeDfWriter().write_table(
    df=df,
    fully_qualified_table_name="STORAGE_DATABASE.CPW_DATA.MASTER_ANGLER_AWARD",
    overwrite=False,
)

fish_species = [
    "Trout: Brook",
    "Trout: Brown",
    "Trout: Cutbow",
    "Trout: Cutthroat",
    "Trout: Golden",
    "Trout: Lake",
    "Trout: Rainbow",
    "Trout: Snake River Cutthroat",
    "Trout: Tiger",
    "Trout: Unspecified",
]

for species in fish_species:
    fully_qualified_name = f"STORAGE_DATABASE.CPW_DATA.{species}"

    # Need to make the species scraper work the same way
    raw_species_data = colorado_fishing_atlas.fishing_atlas_scraper(species).execute()

    df = clean_atlas_data.process_all_location_data(raw_species_data)

    snowflake_writer.SnowflakeDfWriter().write_table(
        df=df, fully_qualified_table_name=fully_qualified_name, overwrite=True
    )
