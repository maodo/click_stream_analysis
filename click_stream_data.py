import random
from datetime import datetime, timedelta
import shortuuid
import json
import time
# Each event can include attributes like:
# User ID: Unique identifier for the user. - We have 100 users visiting our website
# Session_ID: Unique identifier for a session (a series of actions grouped within a time frame).
# Timestamp: Exact time of the event.
# Event_Type: Action taken by the user (e.g., page_view, click_button, add_to_cart, purchase).
# URL: URL of the webpage where the event occurred.
# Referrer_URL: The URL that referred the user to the current page.
# User_Agent: Browser and operating system details of the user.
# Device_Type: Type of device used (e.g., Desktop, Mobile, Tablet).
# Location: Geographical location of the user (inferred from IP).
user_ids = [shortuuid.uuid() for _ in range(50)]
website_url = 'https://maodo.com'
page_urls = ["/home", "/products", "/about", "/contact", "/blog"]
product_urls = [f"/product/{product_id}" for product_id in range(20)]
checkout_url = "/checkout"
event_types = ["page_view", "click_button", "add_to_cart", "purchase"]
referrers = ["https://google.com", "https://bing.com", "https://amazon.com","https://youtube.com", "Direct"]
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0)",
    "Mozilla/5.0 (Macintosh)",
    "Mozilla/5.0 (Linux)",
    "Mozilla/5.0 (iPhone)"
]
devices = ["Desktop", "Mobile", "Tablet"]
locations = ["Dakar, Senegal","New York, USA", "London, UK", "Paris, France", "Berlin, Germany"]
def generate_clickstream_event():
    user = random.choice(user_ids)
    session_id = f"sess_{random.randint(1, 1000):03}"
    device = random.choice(devices)
    location = random.choice(locations)
    user_agent = random.choice(user_agents)
    referrer_url = random.choice(referrers)
    random_date = datetime.now() - timedelta(days=random.randint(0,365))
    timestamp = int(random_date.timestamp()*1000)
    event =random.choice(event_types)
    if(event == "add_to_cart"):
        url = f"{website_url}{random.choice(product_urls)}"
    elif(event == "purchase"):
        url = f"{website_url}{checkout_url}"
    else:
        url = f"{website_url}{random.choice(page_urls)}"
    return {
        "user_id":user,
        "session_id":session_id,
        "timestamp":timestamp,
        "event_type":event,
        "url":url,
        "referrer_url":referrer_url,
        "user_agent":user_agent,
        "device_type": device,
        "location":location
    }
# Stream the data
def stream_clickstream_data(duration_seconds=10, interval_seconds=1):
    start_time = time.time() 
    print(start_time)
    while time.time()  - start_time < duration_seconds:
        event = generate_clickstream_event()
        print(json.dumps(event))
        time.sleep(interval_seconds)

stream_clickstream_data()