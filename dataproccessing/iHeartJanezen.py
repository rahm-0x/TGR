import requests
import json
import time
from datetime import datetime

# ZenRows API Key and endpoint
ZENROWS_API_KEY = "6d31be69d8761dff28f1aad409b3b563f9a2b9f9"
zenrows_url = "https://api.zenrows.com/v1/"
output_file = "iheartjane_data_output.txt"

# Set up ZenRows request parameters
headers = {
    "Content-Type": "application/json"
}
params = {
    "apikey": ZENROWS_API_KEY,
    "js_render": "true",
    "json_response": "true"
}

# Define iHeartJane stores and snapshot time
store_ids = {
    888: "Cookies on the Strip",
    886: "Rise-Tropicana",
    1885: "Rise-Durango",
    # 1718: "Rise-Rainbow",
    # 887: "Rise-Henderson",
    # 1474: "Rise-Spanish Springs",
    # 1475: "Rise-Carson City",
    # 3417: "Rise-Reno",
    # 5267: "Rise-Nellis",
    # 5429: "Rise-Craig",
    # 2747: "Zen Leaf-North Las Vegas",
    # 1641: "Zen Leaf-Post",
    # 3702: "Zen Leaf-Flamingo",
    # 762: "Zen Leaf-Reno",
    # 777: "Zen Leaf-Carson City",
    # 3013: "The Source-Henderson",
    # 3012: "The Source-Rainbow",
    # 3104: "The Source-Northwest",
    # 4606: "The Source-Pahrump",
    # 1649: "Oasis Cannabis",
    # 4282: "Vegas Treehouse",
    # 1989: "Reef Sparks",
    # 2014: "Reef Sun Valley",
    # 1856: "Reef Reef North Las Vegas",
    # 1988: "Reef Reef (Western Ave.)"
}
snapshot_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

with open(output_file, "w") as file:
    for store_id, store_name in store_ids.items():
        file.write(f"Processing store ID: {store_id} - {store_name}\n")
        page = 0
        
        while True:
            file.write(f"  Processing page: {page}\n")
            data_payload = json.dumps({
                "query": "",
                "filters": f"store_id:{store_id}",
                "hitsPerPage": 48,
                "page": page,
                "userToken": "tMwhGOkA1eGNh43F38Y7f"
            })

            response = requests.post(
                f"{zenrows_url}?url=https://search.iheartjane.com/1/indexes/menu-products-production/query",
                headers=headers,
                params=params,
                data=data_payload
            )
            time.sleep(2)

            if response.status_code == 200:
                data = response.json()
                products = data.get("hits", [])
                
                if not products:
                    file.write(f"No more products found on page {page}. Ending scrape for {store_name}.\n")
                    break

                file.write(f"  Records pulled: {len(products)}\n")
                
                for product in products:
                    product_name = product.get("name", "Unknown Name")
                    price = product.get("bucket_price", "N/A")
                    quantity = product.get("max_cart_quantity", "N/A")
                    kind = product.get("kind", "N/A")
                    thc_content = product.get("percent_thc", "N/A")
                    brand = product.get("brand", "Unknown Brand")
                    
                    # Replace latitude and longitude with store name
                    location = store_name

                    # Write each product's details to the file
                    file.write(f"    Snapshot Time: {snapshot_time}\n")
                    file.write(f"    Product Name : {product_name}\n")
                    file.write(f"    Price        : ${price}\n")
                    file.write(f"    Quantity     : {quantity}\n")
                    file.write(f"    Type         : {kind}\n")
                    file.write(f"    THC Content  : {thc_content}%\n")
                    file.write(f"    Brand        : {brand}\n")
                    file.write(f"    Location     : {location}\n")
                    file.write("    " + "-" * 40 + "\n")

                page += 1
            else:
                file.write(f"Request failed with status code {response.status_code}\n")
                file.write(response.text + "\n")
                
                if response.status_code == 422:
                    file.write("Encountered proxy error. Waiting before retrying...\n")
                    time.sleep(5)
                else:
                    break  # Stop if the error is unrelated to proxies

print(f"Data saved to {output_file}.")
