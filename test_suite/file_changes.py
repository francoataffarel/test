import sys
import os
import importlib
from sys import exit
import pathlib
from .validation import RNRValidation
from .dao import CSVData
from multiprocessing import Process

rnr_spider = importlib.import_module(
    "mensa-scraper.rnr.rnr.spiders.rnr_spider"
).RnRSpider


processes = []


def run_crawler(argument_dict, file_name):
    arguments = " "
    outputfile_name = f"../../../test_suite/output_data/{file_name}"
    for key in argument_dict:
        arguments += f"-a {key}='{argument_dict[key]}' "
    if argument_dict["marketplace"] == "myntra.com":
        arguments += "-s COOKIES_ENABLED=True"
    os.system(
        "cd .github/../"
        + f"mensa-scraper/{argument_dict['project']}/{argument_dict['project']}/"
        + "&& scrapy crawl "
        + argument_dict["spider"]
        + arguments
        + " -O "
        + outputfile_name
        + "  -s LOG_LEVEL='INFO' "
    )


if __name__ == "__main__":
    changed_files = sys.argv
    try:
        os.makedirs(".github/../test_suite/output_data")
    except FileExistsError:
        exit(1)
    #############################################
    # Fetch Jobs
    #############################################
    if 'ALL_FILES' in changed_files:
        projects = ['.github/mensa-scraper/rnr/rnr/spiders/marketplace']
        print("projects",projects,os.system("pwd && ls -al"))
        changed_files = []
        for project in projects:
            all_files = [str(file) for file in list(pathlib.Path(project).glob('*.py'))]
            changed_files = changed_files + all_files
    
    for i in range(len(changed_files)):
        file_name = changed_files[i].split("/")
        if file_name[-2] == "marketplace" and file_name[1] == "rnr":
            marketplace = file_name[-1].split(".py")[0]
            file_details = {
                "project_name": file_name[1],
                "marketplace": marketplace.replace("_", ".")
                if "_" in marketplace
                else marketplace + ".com",
            }
            csv_data = CSVData(
                marketplace=file_details["marketplace"],
                project=file_details["project_name"],
            )
            marketplace_list = csv_data.get_data()
            for i in range(len(marketplace_list)):
                arguments_dict = {
                    "spider": "rnr_spider",
                    "start_urls": marketplace_list[i][4]
                    .split("\n")[0]
                    .split("start_urls=")[1],
                    "brand": marketplace_list[i][2],
                    "category": marketplace_list[i][4]
                    .split("\n")[1]
                    .split("category=")[1],
                    "marketplace": marketplace_list[i][-2],
                    "project": file_details["project_name"],
                    "mode": 2,
                    "review_capture_duration":"7",
                }
                if 'amazon' in arguments_dict['marketplace'] or 'myntra' in arguments_dict['marketplace']:
                    arguments_dict["proxy"] = True
                file_name = (
                    arguments_dict["project"]
                    + "_"
                    + arguments_dict["brand"].replace(" ", "_")
                    + "_"
                    + arguments_dict["marketplace"].replace(".", "_")
                    + "_"
                    + arguments_dict["category"].replace(" ", "_")
                    + ".json"
                )
                p = Process(target=run_crawler, args=(arguments_dict, file_name))
                processes.append(p)

    if len(processes) == 0:
        print("No changes, skipping integration test")
        exit(0)
    
    #############################################
    # Run Jobs
    #############################################

    for p in processes:
        p.start()

    for p in processes:
        p.join()
    
    #############################################
    # Validate Results
    #############################################
    rnr_validation = RNRValidation()
    if rnr_validation.validate_schema() == False:
        print("Integration Tests Failed, exiting the workflow")
        exit(1)
