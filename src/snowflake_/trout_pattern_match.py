"""
"""

import sys
import logging
import pandas as pd
import recordlinkage
from snowflake.snowpark import Session

logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(filename)s] [%(funcName)20s()] [%(levelname)s] - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("STORAGE_DATABASE.CPW_DATA.LOG_OUTPUTS")


def _clean_data(df: pd.DataFrame, cols=[str]) -> None:
    """Before comparing text values it is important that there is some cleaning
    done to improve the success rate of fuzzy matching. This function will lowercase all
    strings then remove non characters such as periods and hashtags.

    :param session: live connection to snowflake database
    :param df: current dataframe with columns to clean
    :param cols: one or more columns to cleaning within the dataframe
    """
    for col in cols:
        clean_col_name = col + "_clean"
        df[clean_col_name] = df[col].str.lower()

        df[clean_col_name] = df[clean_col_name].str.replace(
            "(lake|reservoir)", "", regex=True
        )

        df[clean_col_name] = (
            df[clean_col_name].str.replace("[^a0-zA9-Z ]", "", regex=True)
        ).str.strip()

    logger.info("Cleaned Columns")


def _clean_trout_species(df: pd.DataFrame) -> None:
    """Trout species are stored in the Fishing Atlas as "<trout species> Trout"
    so we need to remove " Trout" to so the species name is the same between both dataframes.
    This will be helpful for indexing on trout name in a later function.

    :param df: fishing atlas dataframe with trout column to clean
    """
    df["species_clean"] = df.species.str.replace(" Trout", "")

    logger.info("Cleaned Atlas species name")


def _pattern_match_data(
    master_angler_df: pd.DataFrame, species_df: pd.DataFrame
) -> pd.DataFrame:
    """Use recordlinge to create all possible matches between each tables ids where they have the same
    value for the body of water and type of fish.

    :param master_angler_df: contains master angler data from the website
    :param species_df: contains atlas data from the website.

    """
    master_angler_df.set_index("master_angler_award_id", inplace=True)
    species_df.set_index("all_species_id", inplace=True)
    indexer = recordlinkage.Index()

    indexer.block("location_clean", "water_clean")
    indexer.block("location_clean", "property_name_clean")
    indexer.block("species_clean", "main_species")

    candidate_links = indexer.index(master_angler_df, species_df)

    compare = recordlinkage.Compare()

    compare.string(
        "location_clean", "water_clean", method="jarowinkler", label="jaro_comparison"
    )
    compare.string(
        "location_clean",
        "water_clean",
        method="damerau_levenshtein",
        label="levenshtein_comparison",
    )

    # The comparison vectors
    compare_vectors = compare.compute(candidate_links, master_angler_df, species_df)

    logger.info("Successfully compared data")

    return compare_vectors


def _select_best_match(compare_vectors: pd.DataFrame) -> pd.DataFrame:
    """There can be more than 1 match between the two data sources. This function will first filter to all scores
    over 1.2 which is likely to be a match. Then, will remove duplicate matches so only one is left over.

    :param compare_vectors: pattern match output from comparing two datasets
    """
    comparison_df = compare_vectors.reset_index().copy()

    comparison_df["total_score"] = (
        comparison_df.jaro_comparison + comparison_df.levenshtein_comparison
    )

    best_match_index = (
        comparison_df.groupby(by="master_angler_award_id")
        .total_score.idxmax()
        .to_list()
    )

    final_match_df = comparison_df.loc[best_match_index].copy()

    logger.info("Successfully created final pattern match output")

    return final_match_df


def _add_location_columns(final_df, master_angler_df, species_df):
    """Adding in the columns that were compared. This will include the raw values and the
    cleaned value from each data set.
    """
    export_df = final_df.merge(
        master_angler_df.reset_index()[
            ["master_angler_award_id", "location", "location_clean"]
        ],
        on="master_angler_award_id",
        how="right",
    ).merge(
        species_df.reset_index()[
            ["all_species_id", "main_species", "water", "water_clean"]
        ],
        on="all_species_id",
        how="left",
    )

    export_df = export_df.rename(
        columns={
            "location": "master_location",
            "location_clean": "master_location_clean",
            "water": "species_water",
            "water_clean": "species_water_clean",
        }
    )

    export_df[["jaro_comparison", "levenshtein_comparison", "total_score"]] = export_df[
        ["jaro_comparison", "levenshtein_comparison", "total_score"]
    ].fillna(0)

    return export_df


def _write_table(session: Session, df):
    """Writes a df to a specified table"""
    result = session.write_pandas(
        df,
        table_name="PATTERN_MATCH_OUTPUT",
        database="STORAGE_DATABASE",
        schema="CPW_DATA",
        parallel=4,
        auto_create_table=True,
        overwrite=True,
        table_type="",
    )

    table_name = result.table_name

    logger.info(f"Successfully wrote final output to {table_name}")


def match_fishing_data(session: Session) -> str:
    """This function exists to orchestrate all the other functions."""
    # Only want trout data
    master_angler_df = session.sql(
        """select
            *
        from STORAGE_DATABASE.CPW_DATA.MASTER_ANGLER_AWARD
        where "species" in (
        'Cutbow',
        'Rainbow Trout',
        'Brown Trout',
        'Lake Trout',
        'Brook Trout',
        'Cutthroat (Native) Trout',
        'Tiger Trout',
        'Golden Trout'
        )
        """
    ).to_pandas()
    species_df = session.sql(
        "select * from STORAGE_DATABASE.CPW_DATA.ALL_SPECIES"
    ).to_pandas()

    _clean_data(master_angler_df, ["location"])
    _clean_data(species_df, ["water"])

    _clean_trout_species(master_angler_df)

    compare_vectors = _pattern_match_data(master_angler_df, species_df)

    final_df = _select_best_match(compare_vectors)

    export_df = _add_location_columns(final_df, master_angler_df, species_df)

    _write_table(session, export_df)

    return f"Successfully completed matching process with {len(export_df)}. Review Logs for details."
