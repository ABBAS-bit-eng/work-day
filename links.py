import time
import json
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from pymongo import MongoClient
from urllib.parse import urlparse


def extract_company_name(job_url):
    """Extract company name from Workday job URL"""
    try:
        netloc = urlparse(job_url).netloc  # e.g., picknpay.wd3.myworkdayjobs.com
        company = netloc.split('.')[0]     # picknpay
        return company
    except:
        return None


class ExcelLinkScraper:
    def __init__(self, excel_path, chrome_path):
        self.excel_path = excel_path

        # ✅ Chrome setup with bigger viewport
        chrome_options = Options()
        chrome_options.add_argument("window-size=1600,1000")
        self.driver = webdriver.Chrome(executable_path=chrome_path, options=chrome_options)

        # ✅ Zoom Out to 25% (with fallback)
        time.sleep(2)
        try:
            self.driver.execute_script("document.body.style.zoom='25%'")
            # Fallback agar zoom kaam na kare
            self.driver.execute_script("document.body.style.transform='scale(0.25)'")
            self.driver.execute_script("document.body.style.transformOrigin='0 0'")
            print("✅ Zoom set to 25%")
        except Exception as e:
            print("⚠️ Zoom setting failed:", str(e))

        # ✅ MongoDB connection
        client = MongoClient("mongodb://localhost:27017/")
        db = client["jobs_db"]
        self.collection = db["workday_jobs"]

        # ✅ Track already scraped URLs (from DB)
        self.scraped_urls = set()
        try:
            existing = self.collection.find({}, {"url": 1})
            self.scraped_urls = {doc["url"] for doc in existing if "url" in doc}
            print(f"✅ Loaded {len(self.scraped_urls)} previously scraped jobs from DB.")
        except Exception as e:
            print("⚠️ Error loading from MongoDB:", str(e))

    def read_excel_links(self):
        """Read links from CSV or Excel file"""
        if self.excel_path.endswith(".csv"):
            df = pd.read_csv(self.excel_path)
        else:
            df = pd.read_excel(self.excel_path, engine="openpyxl")

        return df['Link'].dropna().unique().tolist()

    def crawl_page(self, link):
        print(f"\n[+] Opening link: {link}")
        self.driver.get(link)
        time.sleep(2)

        while True:
            try:
                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located((By.XPATH, '//a[@class="css-19uc56f"]'))
                )
            except TimeoutException:
                print("❌ Timeout: No jobs found on this page.")
                break

            # ✅ Scroll to bottom to load all jobs
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            while True:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            # ✅ Collect job links
            job_links = [
                p.get_attribute("href")
                for p in self.driver.find_elements(By.XPATH, '//a[@class="css-19uc56f"]')
                if p.get_attribute("href")
            ]

            for url in job_links:
                if url in self.scraped_urls:
                    print(f"⏭️ Skipping already scraped job: {url}")
                    continue

                item = self.scrape_job_fields(url)
                if item:
                    # ✅ Save immediately to DB
                    self.save_to_db(item)
                    self.scraped_urls.add(url)  # prevent duplicate in same run

            # ✅ Try next page
            try:
                next_button = self.driver.find_element(By.XPATH, '//button[@aria-label="next"]')
                self.driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                time.sleep(1)
                next_button.click()
                time.sleep(3)
            except NoSuchElementException:
                print("✅ No next button found, last page reached.")
                break
            except Exception as e:
                print("⚠️ Error clicking next:", str(e))
                break

    def scrape_job_fields(self, job_url):
        """Open a job page and scrape fields"""
        try:
            self.driver.get(job_url)
            time.sleep(2)

            item = {
                "url": job_url,
                "company_name": extract_company_name(job_url),  # ✅ Only 'picknpay'
                "JobDescription": self.get_text('//h2[@data-automation-id="jobPostingHeader"]'),
                "ApplyLink": self.get_attr('//div[@class="css-b3pn3b"]/a[contains(text(), "Apply")]', 'href'),
                "CompanyLogo": self.get_attr('//a[@data-automation-id="logoLink"]/img', 'src'),
                "Locations": self.get_text('//dt[contains(text(), "locations")]/following-sibling::dd'),
                "TimeType": self.get_text('//dt[contains(text(), "time type")]/following-sibling::dd'),
                "PostedOn": self.get_text('//dt[contains(text(), "posted on")]/following-sibling::dd'),
                "JobRequisitionID": self.get_text('//dt[contains(text(), "job requisition id")]/following-sibling::dd'),
                "End_date": self.get_text('//dt[contains(text(), "time left to apply")]/following-sibling::dd')
            }

            # ✅ Extract JSON-LD datePosted
            try:
                data = self.driver.find_element(By.XPATH, '//script[@type="application/ld+json"]').get_attribute("innerText")
                job_json = json.loads(data)
                item['created_at'] = job_json.get("datePosted")
            except:
                item['created_at'] = None

            print("Scraped:", item['JobDescription'], "| Company:", item['company_name'])
            return item
        except Exception as e:
            print("⚠️ Error scraping job:", str(e))
            return None

    def get_text(self, xpath):
        try:
            return self.driver.find_element(By.XPATH, xpath).text
        except:
            return None

    def get_attr(self, xpath, attr):
        try:
            return self.driver.find_element(By.XPATH, xpath).get_attribute(attr)
        except:
            return None

    def save_to_db(self, job):
        """Save one job to MongoDB"""
        try:
            self.collection.update_one(
                {"url": job["url"]},
                {"$set": job},
                upsert=True
            )
            print(f"✅ Saved job: {job['url']}")
        except Exception as e:
            print("⚠️ Error saving to MongoDB:", str(e))

    def run(self):
        links = self.read_excel_links()
        for link in links:
            self.crawl_page(link)
        self.driver.quit()


if __name__ == "__main__":
    scraper = ExcelLinkScraper(
        excel_path=r"C:\Users\user\Downloads\Workday final without duplicates (1).csv",  # CSV/XLSX
        chrome_path=r"C:\Program Files (x86)\chromedriver.exe"
    )
    scraper.run()
