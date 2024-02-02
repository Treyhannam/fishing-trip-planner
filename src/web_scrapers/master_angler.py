""" Contains functions specific to scraping data from Colorado Parks and Wildlife (CPW) Master angler webpage.

The pages General format is as follows

[2021] [2022] [2023]
_____________________________________________________________
|Angler |Species    |Length |Location   |Date      |Released|
|-----------------------------------------------------------|
|John   |Catfish    |23     |Wash Park  |June/2023 |Yes     |
|___________________________________________________________|
        [<<] [<] [...] [1] [2]... [6] [...] [>] [>>]

- The years tabs allow users to flip between each years data
- Bottom tables allow users to flip between data within each year

The functions are dedication paginating through all the data and storing it within a list. From there the list of data can be passed on to 
other functions to process and store the data.
"""

import re
import sys
import logging
from datetime import datetime

import playwright
from playwright.sync_api import sync_playwright

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(filename)s] [%(funcName)20s()] [%(levelname)s] - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


class MasterAnglerScraper:
    def adjust_xpath(
        self, xpath: str, page: playwright.sync_api._generated.Page
    ) -> str:
        """Given an xpath for the master angler next page tab. This function will chech the xpath text.
        If it's a number it will check to the right, if it's a non digit it will check left.

        This is the general pattern [<<] [<] [...] [1] [2]... [6] [...] [>] [>>]
                                                                        ^
        To get the next arrow [>] we want to move to the right so long as it's not [>>]

        :param xpath: the start_xpath parameter, //*[@id="thisTable-2023"]/tfoot/tr/td/div/ul/li[11]/a.
        :param page: the playwright page object we are using to interact with the website

        :return: a new xpath that points to the next page button [>]
        """
        # Get the element index location
        elt = re.findall(r"\[\d+\]", xpath)[0]

        # Get the current index number
        number = int(re.findall("\\d+", elt)[0])

        try:
            # Get the text of our xpath
            xpath_text = page.query_selector(xpath).inner_text()

            if xpath_text == "›":
                logger.info(f"Found correct xpath: {xpath}")
                return xpath
            else:
                # If it's the last element we go down/ to the left
                if xpath_text == "»":
                    next_xpath = xpath.replace(elt, "[" + str(number - 1) + "]")
                # it's not a digit so we go up/ to the right
                else:
                    next_xpath = xpath.replace(elt, "[" + str(number + 1) + "]")

                logger.debug(
                    f"index = {number}, Next xpath = {next_xpath}, xpath_text={xpath_text}"
                )

            return self.adjust_xpath(next_xpath, page)

        except AttributeError:
            # if it's none we need to restart, otherwise script checks for nothing and misses data
            next_xpath = xpath.replace(elt, "[3]")

            return self.adjust_xpath(next_xpath, page)

    def scrape_website(self, playwright: sync_playwright) -> list:
        """Opens Master angler website and scrapes data from each page. This will iterate through each year and then
        iterate through each page of results within each year.
        """
        chromium = playwright.chromium

        browser = chromium.launch(headless=False, args=["--start-fullscreen"])

        page = browser.new_page()

        url = "https://cpw.state.co.us/learn/Pages/MasterAngler.aspx"

        page.goto(url)

        # Start with the first tab
        next_year_exists, year_tab, current_year = (
            True,
            1,
            datetime.today().strftime("%Y"),
        )

        records = []
        # As long as we find a year that exists...
        while next_year_exists == True:
            # Check that the page loads, xpath notes the tab number
            element = page.wait_for_selector(
                f'//*[@id="ui-id-{year_tab}"]', timeout=30_000
            )

            tab_year = element.inner_text()

            logger.info(f"Scraping data for year {tab_year}")

            element.click()

            next_element_exists, page_number = True, 1

            start_xpath, data_xpath, row_tabs = (
                f'//*[@id="thisTable-{tab_year}"]/tfoot/tr/td/div/ul/li[11]/a',
                f'//*[@id="thisTable-{tab_year}"]/tbody',
                f'//*[@id="thisTable-{tab_year}"]/tfoot/tr/td/div/ul',
            )

            while next_element_exists == True:
                # Get the table data
                page_records = page.query_selector(data_xpath).inner_text()
                records.append(page_records)

                try:
                    next_page_icon = page.query_selector(start_xpath).inner_text()

                    next_page_exist = next_page_icon == "›"

                except AttributeError as e:
                    logger.warning(f"Experiencing an AttributeError: {e}")

                    logger.info("Attempting to recolve by adjusting xpath...")

                    start_xpath = self.adjust_xpath(start_xpath, page)

                    next_page_exist = (
                        page.query_selector(start_xpath).inner_text() == "›"
                    )

                    logger.info("Successfully resolved by adjusting xpath!")

                if next_page_exist:
                    page.query_selector(start_xpath).click()

                    logging.debug(
                        f"Page Number {page_number}, Next Page Icon {next_page_icon} Correct Icon? {next_page_exist}"
                    )

                    page_number += 1
                # the > character has shifted we want to adjust the xpath so we can continue paginating
                else:
                    next_element_same_row = (
                        str(page_number + 1)
                        in page.query_selector(row_tabs).inner_text()
                    )

                    logging.debug(
                        f"Does the next element ({page_number}) exist on the same row? {next_element_same_row}"
                    )

                    if next_element_same_row:
                        logging.warning(
                            "Cannot find next page tab with current xpath, trying to resolved by adjusting xpath..."
                        )
                        start_xpath = self.adjust_xpath(start_xpath, page)
                    else:
                        next_element_exists = False

                next_page_check = (
                    str(page_number) in page.query_selector(row_tabs).inner_text()
                )
                logging.debug(f"Next number exist? {next_page_check}")
                if not next_page_check:
                    next_element_exists = False

            logger.info(f"There were {page_number-1} pages for year {tab_year}")

            # Move on to the next tab
            year_tab += 1
            if current_year == tab_year:
                next_year_exists = False
            else:
                next_year_exists = True

        browser.close()

        return records

    def execute(self):
        """Creates a playwright instance and passes that into scrape website"""
        with sync_playwright() as playwright:
            raw_data = self.scrape_website(playwright)

        return raw_data
