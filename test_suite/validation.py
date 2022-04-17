import json
import jsonschema
import os
from sys import exit
from jsonschema import validate
from .base.factories import Validator

rnr_review_schema = json.load(
    open(os.path.join(os.path.dirname(__file__), "schema/rnr_review_schema.json"))
)
rnr_product_schema = json.load(
    open(os.path.join(os.path.dirname(__file__), "schema/rnr_product_schema.json"))
)


class RNRValidation(Validator):
    def validate_schema(self):
        for file in os.listdir(os.path.join(os.path.dirname(__file__), "output_data/")):
            try:
                brand_data = json.load(
                    open(
                        os.path.join(os.path.dirname(__file__), "output_data/" + file),
                        "r",
                    )
                )
            except:
                print(file + " JSON File is empty")
                return False
            reviews = []
            products = []
            for i in range(len(brand_data)):
                if "reviews_href" in brand_data[i]:
                    products.append(brand_data[i])
                else:
                    reviews.append(brand_data[i])
            if len(products) == 0:
                print(file + " No products found")
                return False
            try:
                for product in products:
                    if product["price"] == "0":
                        print(f"No price found for {product['product_href']}")
                    validate(
                        instance=product,
                        schema=rnr_product_schema,
                        format_checker=jsonschema.FormatChecker(),
                    )
                if len(reviews) != 0:
                    for review in reviews:
                        validate(
                            instance=review,
                            schema=rnr_review_schema,
                            format_checker=jsonschema.FormatChecker(),
                        )
                else:
                    print(file + " No reviews found")
            except jsonschema.exceptions.ValidationError as validation_error:
                print("Validation Error: ", validation_error)
                return False
        return True
