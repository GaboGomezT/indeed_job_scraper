# Import scrapy
import scrapy
# Import the CrawlerProcess: for running the spider
from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import CloseSpider
import pandas as pd
# Create the Spider class
class IndeedSpider(scrapy.Spider):
    name = "IndeedSpider"
    # start_requests method

    def start_requests(self):
        global job_title
        yield scrapy.Request(url=f'https://www.indeed.com/jobs?q={job_title}&l=United+States', callback=self.parse_pagination)

    # First parsing method
    def parse_pagination(self, response):
        paths = response.xpath('//a[@class="jobtitle turnstileLink "]/@href').extract()
        current_page = response.xpath('//b[@aria-current="true"]/@aria-label').extract_first()
        for job_key in paths:
            yield response.follow(url=f"https://www.indeed.com/viewjob{job_key[7:]}&sort=date", callback=self.get_job_data, meta={'current_page': current_page})

        next_page_link_path = response.xpath('//ul[@class="pagination-list"]//a[@aria-label="Next"]/@href').extract_first()
        if next_page_link_path:
            yield response.follow(url=f"https://www.indeed.com{next_page_link_path}", callback=self.parse_pagination)

    def get_job_data(self, response):
        global job_postings

        job_url = response.xpath('//meta[@id="indeed-share-url"]/@content').extract_first()
        posted_job_title = response.xpath('//h1[@class="icl-u-xs-mb--xs icl-u-xs-mt--none jobsearch-JobInfoHeader-title"]/text()').extract_first()
        principal_company_name = response.xpath('//div[@class="icl-u-lg-mr--sm icl-u-xs-mr--xs"]/a/text()').extract_first()
        secondary_company_name = response.xpath("//div[contains(@class, 'jobsearch-InlineCompanyRating')]/div[1]/text()").extract_first() # in case the principal_company_name doesn't catch the name this xpath probably will
        location = response.xpath('//div[@class=" jobsearch-CompanyInfoWithoutHeaderImage jobsearch-CompanyInfoWithReview"]/div[1]/div[1]/div[1]/div[4]/text()').extract_first()
        rating = response.xpath('//div[@class="icl-Ratings-starsCountWrapper"]/@aria-label').extract_first()
        rating_count = response.xpath('//div[@class="icl-Ratings-count"]/text()').extract_first()
        description = response.xpath('//div[@id="jobDescriptionText"]').extract_first()

        data = {
            "job_url": job_url,
            "posted_job_title": posted_job_title,
            "principal_company_name": principal_company_name,
            "secondary_company_name": secondary_company_name,
            "location": location,
            "rating": rating,
            "rating_count": rating_count,
            "description": description,
            "current_page": response.meta.get('current_page'),
        }        

        job_postings.append(data)        


job_postings = []
job_title = "machine learning engineer"
# job_title = input("What job are you looking for?")
job_title.replace(' ', '+')
process = CrawlerProcess()
process.crawl(IndeedSpider)
process.start()

df = pd.DataFrame(job_postings)
df.to_csv("job_postings.csv", index=False)

