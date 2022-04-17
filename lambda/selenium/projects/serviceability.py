from email import message
import json
from utils import ExceptionHandler
from http import HTTPStatus
from seleniumdriver import WebDriverWrapper
from selenium.webdriver.common.by import By
from datetime import datetime, timedelta, time
from selenium.webdriver.common.keys import Keys
import calendar
import re
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class Serviceability:

    def marketplace_handler(self, marketplace, **kwargs):
        if "amazon.in" == marketplace:
            return AmazonIN(marketplace).run_job(**kwargs)
        elif "flipkart.com" == marketplace:
            if 'option' not in kwargs:
                raise ExceptionHandler(code=HTTPStatus.BAD_REQUEST, message=json.dumps({"message": "Flipkart requires option of UI"}))
            return Flipkart(marketplace).run_job(**kwargs)
        raise ExceptionHandler(code=HTTPStatus.BAD_REQUEST, message=json.dumps({"message": "No marketplace found"}))


class AmazonIN:

    def __init__(self, marketplace) -> None:
        self.driver = WebDriverWrapper(marketplace)
    
    def get_delivery_date(self, delivery_date_text) -> datetime:
        if 'Tomorrow' in delivery_date_text or 'tomorrow' in delivery_date_text:
            #e.g. case Tommorow, Thursday
            return datetime.today() + timedelta(1)
        elif '-' in delivery_date_text:
            #e.g. Feb 6 - 8
            delivery_date_text = delivery_date_text.split("-")[0].strip()
            month, date = delivery_date_text.split(" ")
            date = int(date.strip())
            if month.strip() in list(calendar.month_abbr):
                month = list(calendar.month_abbr).index(month.strip())
            else:
                month = list(calendar.month_name).index(month.strip())
        else:
            #e.g. Wednesday, March 2
            day, date_text = delivery_date_text.split(",")
            date_text = date_text.strip()
            date_text = date_text.split(" ")
            date = int(date_text[-1].strip())
            month_text = date_text[0].strip()
            if month_text in list(calendar.month_abbr):
                month = list(calendar.month_abbr).index(month_text)
            else:
                month = list(calendar.month_name).index(month_text)
        if datetime.today().month == 12 and month != datetime.today().month:
            #next year is the Delivery
            return datetime(day=date, month=month, year=datetime.today().year+1)
        else:
            return datetime(day=date, month=month, year=datetime.today().year)        
    
    def run_job(self, **kwargs):
        try:
            pincode = kwargs['pincode']
            product_url = kwargs['product_url']
            self.driver.delete_cookies()
            self.driver.get_url(url=product_url)
            self.driver.wait_until_element_clickable(By.ID, 'contextualIngressPtLabel_deliveryShortLine', delay=10)
            self.driver.execute_script("document.getElementById('contextualIngressPtLabel_deliveryShortLine').click()")
            self.driver.wait_until_element_clickable(By.ID, 'GLUXZipUpdateInput')
            self.driver.set_input_value_id('GLUXZipUpdateInput', pincode)
            self.driver.execute_script("document.querySelector('#GLUXZipUpdate > span > input').click()")
            self.driver.wait_until_element(By.XPATH, '//div[@id="mir-layout-DELIVERY_BLOCK-slot-DELIVERY_MESSAGE"]/b[contains(text(), ",")]', delay=10)
            self.driver.wait_until_element(By.XPATH, f'//div[@id="contextualIngressPtLabel_deliveryShortLine"]/span[2][contains(text(),{pincode})]')
            pincode = self.driver.find_css('#contextualIngressPtLabel_deliveryShortLine > span:nth-child(2)').text.strip()
            product_name = self.driver.find_css('#productTitle').text.strip()
            pincode = str(pincode.split(" ")[-1]).encode('ascii', 'ignore').decode('utf-8')
            delivery_date_text = self.driver.find_css('#mir-layout-DELIVERY_BLOCK-slot-DELIVERY_MESSAGE > b:last-of-type').text.strip()
            delivery_date = datetime.combine(self.get_delivery_date(delivery_date_text), time.min)
            extracted_date = datetime.combine(datetime.today(), time.min)
            days_for_delivery = delivery_date - extracted_date
            delivery_date = delivery_date.strftime('%Y%m%d')
            extracted_date = extracted_date.strftime('%Y%m%d')

            response = {
                "product_url": product_url,
                "pincode": pincode,
                "product_name": product_name,
                "delivery_date": delivery_date,
                "extracted_date": extracted_date,
                "days_for_delivery": days_for_delivery.days
            }

            return response

        except Exception as e:
            logger.error(e)
            raise ExceptionHandler(code=HTTPStatus.INTERNAL_SERVER_ERROR, message=json.dumps({"message": "Internal error occored"}))

        finally:
            self.driver.close()            

class Flipkart:

    def __init__(self, marketplace) -> None:
        self.driver = WebDriverWrapper(marketplace)

    def get_delivery_date(self, delivery_date_text) -> datetime:
        if 'Tomorrow' in delivery_date_text or 'tomorrow' in delivery_date_text:
            #e.g. case Tommorow, Thursday
            return datetime.today() + timedelta(1)
        elif 'Today' in delivery_date_text or 'today' in delivery_date_text:
            #e.g 11 AM, Today
            return datetime.today()
        elif 'Days' in delivery_date_text:
            #e.g. case 2 Days, Friday
            date_text, day = delivery_date_text.split(",")
            date_text = date_text.strip()
            date_text = date_text[:date_text.find('Days')].strip()
            return datetime.today() + timedelta(int(date_text))
        elif 'days' in delivery_date_text:
            #e.g. case 6-7 days and 3 days
            date_text = delivery_date_text[:delivery_date_text.find('days')].strip().split("-")
            return datetime.today() + timedelta(int(date_text[-1]))
        else:
            #e.g. 27 Dec, Monday
            date_text, day = delivery_date_text.split(",")
            date_text = date_text.strip()
            date_text = date_text.split(" ")
            date = int(date_text[0])
            month = list(calendar.month_abbr).index(date_text[1])
            if datetime.today().month == 12 and month != datetime.today().month:
                #next year is the Delivery
                return datetime(day=date, month=month, year=datetime.today().year+1)
            else:
                return datetime(day=date, month=month, year=datetime.today().year)

    
    def run_job(self, **kwargs):                
        try:
            pincode = kwargs['pincode']
            product_url = kwargs['product_url']
            self.driver.delete_cookies()
            self.driver.get_url(url=product_url)
            if kwargs['option'] == 'fashion':
                logger.info('Fashion UI Option selected')
                self.driver.wait_until_element_clickable(By.CLASS_NAME, 'cfnctZ', delay=10)
                self.driver.execute_script("document.getElementsByClassName('cfnctZ')[0].click()")
                self.driver.set_input_value_class('cfnctZ', pincode)
                self.driver.execute_script("document.getElementsByClassName('UgLoKg')[0].click()")
                self.driver.wait_until_element(By.CLASS_NAME, '_12cXX4')
                pincode = self.driver.find_css('._12cXX4').text
            else:
                logger.info('Non-Fashion UI Option selected')
                self.driver.wait_until_element_clickable(By.ID, 'pincodeInputId', delay=10)
                self.driver.execute_script("document.getElementById('pincodeInputId').click()")
                for i in range(6):
                    self.driver.set_input_value_id('pincodeInputId', Keys.BACKSPACE)
                self.driver.set_input_value_id('pincodeInputId', pincode)
                self.driver.execute_script("document.getElementsByClassName('_2P_LDn')[0].click()")
                self.driver.wait_until_element_staleness(By.XPATH, f'//input[@class="_36yFo0"]')
                self.driver.wait_until_element(By.XPATH, f'//input[@class="_36yFo0" and @value="{pincode}"]')
                pincode = self.driver.find_xpath('//input[@class="_36yFo0"]').get_attribute("value")
            pincode = re.sub(r'\D', '', pincode)
            delivery_date_text = self.driver.find_class('_1TPvTK')
            if delivery_date_text is not None and len(delivery_date_text) > 0:
                product_name = self.driver.find_css('.B_NuCI').text
                delivery_date = datetime.combine(self.get_delivery_date(delivery_date_text[0].text.strip()), time.min)
                extracted_date = datetime.combine(datetime.today(), time.min)
                days_for_delivery = delivery_date - extracted_date
                delivery_date = delivery_date.strftime('%Y%m%d')
                extracted_date = extracted_date.strftime('%Y%m%d')
                response = {
                    "product_url": product_url,
                    "pincode": pincode,
                    "product_name": product_name,
                    "delivery_date": delivery_date,
                    "extracted_date": extracted_date,
                    "days_for_delivery": days_for_delivery.days
                }
                return response

            raise Exception(message="Delivery date text not found, retry request.")                

        except Exception as e:
            logger.error(e)
            raise ExceptionHandler(code=HTTPStatus.INTERNAL_SERVER_ERROR, message=json.dumps({"message": "Internal error occored"}))

        finally:
            self.driver.close()
        
