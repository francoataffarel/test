### Local setup

1. Install Docker

### Command to run job

1. `cd rnr`
2. `scrapy crawl rnr_spider -a <arg1=val1> -a <arg2=val2>`

### Arguments to pass

1. `brand` (required)
2. `marketplace` (required)
3. `start_urls` (required) (comma seperated and within double quotes)
4. `mode` valid values `Full` (default) or `Test`
5. `review_capture_duration` (default `15`)
6. `scrape_reviews` (default `True`)
7. `scrape_products` (default `True`)

### See items locally

1. Save the scraped items to a JL file
2. append `-O filename.jl` to scrapy crawl command

### Example

1. `scrapy crawl rnr_spider -a brand=villain -a marketplace=amazon.in -a start_urls="https://www.amazon.in/stores/Villain/page/36A1E5A8-4E4B-4614-BEBD-E0813E35A215?ref_=ast_bln" -O filename.jl`
