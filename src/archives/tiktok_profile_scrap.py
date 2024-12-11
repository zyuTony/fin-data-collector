"""
This script extract a tiktok profile and check all the video titles, hashtags, and views
1. scroll to bottom to make sure all the contents have loaded.
2. copy the body element as a whole and paste into a .html file.
3. run the below file to extract video title, hashtags, and video view.
"""

from bs4 import BeautifulSoup
import pandas as pd
from utlis.tiktok_utlis import remove_emojis, convert_views 
import re

def remove_emojis(text):
    emoji_pattern = re.compile(
        "["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)

def convert_views(view_str):
    if 'K' in view_str:
        return int(float(view_str.replace('K', '')) * 1000)
    elif 'M' in view_str:
        return int(float(view_str.replace('M', '')) * 1000000)
    else:
        return int(view_str)
profile_name='stockbuster'

# Load the HTML content from a local file
with open(DATA_FOLDER+f'/{profile_name}.html', 'r', encoding='utf-8') as file:
    html_content = file.read()

# load content into bs4
soup = BeautifulSoup(html_content, 'html.parser')
# find all desired things matching class_
video_containers = soup.find_all("div", class_="css-x6y88p-DivItemContainerV2")
print(len(video_containers))

videos = []
for container in video_containers:
    try:
        video_url = container.find("a").get('href')
        title = container.find("div",{"data-e2e": "user-post-item-desc"})['aria-label']
        clean_title = remove_emojis(title)
        video_views_str = container.find("strong", class_="video-count").text
        video_views = convert_views(video_views_str)
        hashtags = [a.get('href').replace('/tag/', '') for a in container.find_all("a",{"data-e2e": "search-common-link"})] 
        videos.append({"url": video_url, "title": clean_title, "views": video_views, "hashtags": hashtags})
    except Exception as e:
        print(f"Error parsing container: {e}")

# save to csv
df = pd.DataFrame(videos)
df.to_csv(f"home/ec2-user/financial_database/backend/{profile_name}.csv", index=False)

print(f"Data saved to {profile_name}.csv")
