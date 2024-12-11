"""
This script extract a tiktok profile and check all the video titles, hashtags, and views
1. scroll to bottom to make sure all the contents have loaded.
2. copy the body element as a whole and paste into a .html file.
3. run the below file to extract video title, hashtags, and video view.
"""

from bs4 import BeautifulSoup
import pandas as pd
from utlis.tiktok_utlis import remove_emojis, convert_views 

profile_name='jrny'

# Load the HTML content from a local file
with open(DATA_FOLDER+f'/{profile_name}.html', 'r', encoding='utf-8') as file:
    html_content = file.read()

# load content into bs4
soup = BeautifulSoup(html_content, 'html.parser')
# find all desired things matching class_
video_containers = soup.find_all('a', id='video-title-link') 
# title = video_containers['title']
# aria_label = video_containers['aria-label'].replace(title, "").strip()
# views_part = aria_label.split('views')[0].split()[-1]
# upload_time = ' '.join(aria_label.split('views')[1:]).strip()
# print(title)
# print(aria_label)
# print(views_part)
# print(upload_time)
print(len(video_containers))

 
videos = []
for container in video_containers:
    try:
        title = container['title']
        aria_label = container['aria-label'].replace(title, "").strip()
        views_part = aria_label.split('views')[0].split()[-1]
        upload_time = ' '.join(aria_label.split('views')[1:]).strip()
        # Calculate days ago from upload time
        days_ago = 0
        if 'years ago' in upload_time:
            years_ago = int(upload_time.split('years ago')[0].strip())
            days_ago += years_ago * 365
            upload_time = upload_time.split('years ago')[1].strip()
        if 'months ago' in upload_time:
            months_ago = int(upload_time.split('months ago')[0].strip())
            days_ago += months_ago * 30
        videos.append({"title": title, "views": views_part, "days_ago": days_ago,"upload_time": upload_time})
    except Exception as e:
        print(f"Error parsing container: {e}")

# save to csv
df = pd.DataFrame(videos)
df.to_csv(f"home/ec2-user/financial_database/backend/{profile_name}.csv", index=False)

print(f"Data saved to {profile_name}.csv")
