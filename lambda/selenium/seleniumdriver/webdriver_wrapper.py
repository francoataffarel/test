from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import random
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import os
from selenium.webdriver.common.proxy import Proxy
import uuid
import shutil
from .driver_constants import chrome_config, custom_fake_user_agents

class WebDriverWrapper:
    def __init__(self, marketplace):
        chrome_options = webdriver.ChromeOptions()

        self._tmp_folder = f'/tmp/{uuid.uuid4()}'

        if not os.path.exists(self._tmp_folder):
            os.makedirs(self._tmp_folder)

        if not os.path.exists(self._tmp_folder + '/user-data'):
            os.makedirs(self._tmp_folder + '/user-data')

        if not os.path.exists(self._tmp_folder + '/data-path'):
            os.makedirs(self._tmp_folder + '/data-path')

        if not os.path.exists(self._tmp_folder + '/cache-dir'):
            os.makedirs(self._tmp_folder + '/cache-dir')
        
        chrome_options.binary_location = '/opt/chrome/stable/chrome'
        chrome_options.add_argument(f'--homedir={self._tmp_folder}')
        chrome_options.add_argument(f'--user-data-dir={self._tmp_folder + "/user-data"}')
        chrome_options.add_argument(f'--data-path={self._tmp_folder + "/data-path"}')
        chrome_options.add_argument(f'--disk-cache-dir={self._tmp_folder + "/cache-dir"}')
        chrome_options.add_argument(
            f'user-agent={random.choice(custom_fake_user_agents)}')

        for option in chrome_config:
            chrome_options.add_argument(option)

        prefs = {"profile.managed_default_content_settings.images": 2}
        chrome_options.add_experimental_option("prefs", prefs)            

        capabilities = DesiredCapabilities.CHROME.copy()
        capabilities['pageLoadStrategy']='none'
        if marketplace == 'amazon.in':
            PROXY = os.getenv("PROXY_SESSION")
            if PROXY:
                proxy_config = {'httpProxy': PROXY, 'sslProxy': PROXY}
                proxy_object = Proxy(raw=proxy_config)
                proxy_object.add_to_capabilities(capabilities)            
        
        self._driver = webdriver.Chrome("/opt/chromedriver/stable/chromedriver",
                chrome_options=chrome_options, desired_capabilities=capabilities)

    def get_url(self, url):
        self._driver.get(url)

    def set_input_value_xpath(self, xpath, value):
        elem_send = self._driver.find_element_by_xpath(xpath)
        elem_send.send_keys(value)

    def set_input_value_class(self, classname, value):
        elem_send = self._driver.find_element_by_class_name(classname)
        elem_send.send_keys(value)

    def set_input_value_id(self, id, value):
        elem_send = self._driver.find_element_by_id(id)
        elem_send.send_keys(value)        

    def click(self, xpath):
        elem_click = self._driver.find_element_by_xpath(xpath)
        elem_click.click()

    def get_inner_html(self, xpath):
        elem_value = self._driver.find_element_by_xpath(xpath)
        return elem_value.get_attribute('innerHTML')
    
    def find_xpath(self, xpath):
        return self._driver.find_element_by_xpath(xpath)

    def find_class(self, classname):
        return self._driver.find_elements_by_class_name(classname)

    def find_id(self, id):
        return self._driver.find_element_by_id(id)

    def find_css(self, selector):
        return self._driver.find_element_by_css_selector(selector)        
    
    def delete_cookies(self):
        self._driver.delete_all_cookies()
    
    def wait_until_element(self, search_by, search_element, delay=5):
        WebDriverWait(self._driver, delay).until(EC.presence_of_element_located((search_by, search_element)))

    def wait_until_element_value(self, search_by, search_element, value, delay=5):
        WebDriverWait(self._driver, delay).until(EC.text_to_be_present_in_element_value((search_by, search_element), value))        

    def wait_until_element_clickable(self, search_by, search_element, delay=5):
        WebDriverWait(self._driver, delay).until(EC.element_to_be_clickable((search_by, search_element)))

    def wait_until_element_staleness(self, search_by, search_element, delay=5):
        WebDriverWait(self._driver, delay).until(EC.staleness_of(self._driver.find_element(search_by, search_element)))
    
    def execute_script(self, script):
        self._driver.execute_script(script)
        
    def close(self):
        # Close webdriver connection
        self._driver.quit()
        shutil.rmtree(self._tmp_folder)