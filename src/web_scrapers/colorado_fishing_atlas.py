import sys
import logging
import time
from datetime import datetime


from playwright.sync_api import sync_playwright

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(filename)s] [%(funcName)20s()] [%(levelname)s] - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


class fishing_atlas_scraper:
    def __init__(self, fish_species):
        self.fish_species = fish_species

    def scrape_website(self, playwright: sync_playwright):
        chromium = playwright.chromium

        browser = chromium.launch(headless=True, args=["--start-fullscreen"])

        page = browser.new_page()

        url = "https://ndismaps.nrel.colostate.edu/index.html?app=FishingAtlas"

        page.goto(url)

        all_records = []

        # Click Agree on terms of service
        page.wait_for_selector(
            '//*[@id="dijit_form_Button_0_label"]', timeout=5_000
        ).click()

        # Change the Location coordinates to units we can use with geopandas or google maps
        location_coordinate_display_xpath = '//*[@id="xycoords"]'

        page.wait_for_selector(location_coordinate_display_xpath, timeout=5_000).click()

        decimal_degrees_xpath = '//*[@id="xycoords_combo"]/option[2]'

        page.wait_for_selector(decimal_degrees_xpath, timeout=5_000).click()

        search_bar_text = "None"

        species_search_bar_xpath = '//*[@id="searchText"]'

        while search_bar_text != self.fish_species:
            time.sleep(0.5)

            logger.debug(
                f"Search bar does not contain: {self.fish_species}. Current text {search_bar_text}. Will try entering text again..."
            )

            # Select Trout speciese
            page.wait_for_selector(species_search_bar_xpath, timeout=5_000).fill(
                self.fish_species
            )

            search_bar_text = page.wait_for_selector(
                species_search_bar_xpath, timeout=5_000
            ).input_value()

        logger.debug(f"Successfully typed {self.fish_species}")

        # having trouble with enter need to make sure it goes through or else it will error out on the first run and then pull the same data if its a 2nd or third run
        page.keyboard.press("Enter")

        features_found_number_xpath = '//*[@id="searchTools"]'

        # Text will look like: Features Found: N, where N is any number
        features_found = page.wait_for_selector(
            features_found_number_xpath, timeout=5_000
        ).inner_text()

        features_number = int(features_found.split(":")[1])

        logger.info(
            f"There are {features_number} results for search term {self.fish_species}"
        )

        for i in range(0, features_number):
            result_location_xpath = (
                '//*[@id="dgrid_1-row-int_placeholder"]/table/tr/td[1]'.replace(
                    "int_placeholder", str(i)
                )
            )

            page.wait_for_selector(result_location_xpath, timeout=10_000).click()

            location_name = page.wait_for_selector(
                result_location_xpath, timeout=10_000
            ).inner_text()

            logger.debug(f"Grabbing data for {location_name}")

            time.sleep(3)

            box = page.wait_for_selector(
                '//*[@id="mapDiv"]', timeout=5_000
            ).bounding_box()

            pop_up_visble = page.query_selector(
                '//*[@id="mapDiv_root"]/div[3]/div[1]'
            ).is_visible()
            while pop_up_visble == False:
                time.sleep(1)

                logger.debug(
                    "The location popup did not appear, waiting 2 seconds then trying again"
                )

                page.mouse.move(
                    box["x"] + box["width"] / 2, box["y"] + box["height"] / 2
                )

                page.mouse.click(
                    box["x"] + box["width"] / 2, box["y"] + box["height"] / 2
                )

                time.sleep(1)

                pop_up_visble = page.query_selector(
                    '//*[@id="mapDiv_root"]/div[3]/div[1]'
                ).is_visible()

            loaction_data_xpath = '//*[@id="mapDiv_root"]/div[3]/div[1]'

            location_data = page.wait_for_selector(
                loaction_data_xpath, timeout=5_000
            ).inner_text()

            if "Loading..." in location_data:
                attempts = 0

                logger.warning(
                    f"Data did not load for {location_name}, waiting then trying again"
                )

                while "Loading..." in location_data and attempts != 5:
                    time.sleep(1)

                    location_data = page.wait_for_selector(
                        loaction_data_xpath, timeout=5_000
                    ).inner_text()

                    attempts += 1

                if attempts == 5:
                    logger.warning(
                        f"Data did not load for {location_name} after 5 attempts"
                    )
                else:
                    logger.info(
                        f"Data successfully loaded for {location_name} after {attempts} attempts"
                    )

            coordinate_data = page.wait_for_selector(
                location_coordinate_display_xpath, timeout=5_000
            ).inner_text()

            # Adding fish species so we can differentiate data when there is more than one body of water
            combined_data = location_name + "XXXX" + location_data + coordinate_data

            all_records.append(combined_data)

            # close the popup
            page.mouse.click(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)

        logger.info(f"Successfully pulled all the data for {self.fish_species}")

        return all_records

    def execute(self):
        with sync_playwright() as playwright:
            raw_data = self.scrape_website(playwright)

        return raw_data
