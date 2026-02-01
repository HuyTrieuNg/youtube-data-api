# Import thư viện cần thiết
import os
from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

YOUTUBE_API_KEY = os.getenv('YOUTUBE_DATA_API_KEY')
if not YOUTUBE_API_KEY:
    raise ValueError("YOUTUBE_DATA_API_KEY không tìm thấy trong file .env")
    
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)


# Hàm search video theo từ khóa
def search_videos(youtube, keyword, max_results=50):
    """
    Tìm kiếm video trên YouTube theo từ khóa
    """
    search_response = youtube.search().list(
        q=keyword,
        part='id,snippet',
        maxResults=max_results,
        type='video',
        order='relevance'
    ).execute()
    
    videos = []
    for item in search_response.get('items', []):
        video_info = {
            'video_id': item['id']['videoId'],
            'title': item['snippet']['title'],
            'channel_title': item['snippet']['channelTitle'],
            'published_at': item['snippet']['publishedAt'],
            'description': item['snippet']['description']
        }
        videos.append(video_info)
    
    return videos


# Hàm lấy comments của video
def get_video_comments(youtube, video_id, max_results=100):
    """
    Lấy comments của một video
    """
    comments = []
    
    try:
        # Lấy comment threads
        request = youtube.commentThreads().list(
            part='snippet,replies',
            videoId=video_id,
            maxResults=max_results,
            textFormat='plainText'
        )
        
        while request:
            response = request.execute()
            
            for item in response['items']:
                # Top-level comment
                top_comment = item['snippet']['topLevelComment']['snippet']
                comment_data = {
                    'video_id': video_id,
                    'comment_id': item['snippet']['topLevelComment']['id'],
                    'author': top_comment['authorDisplayName'],
                    'text': top_comment['textDisplay'],
                    'like_count': top_comment['likeCount'],
                    'published_at': top_comment['publishedAt'],
                    'updated_at': top_comment['updatedAt'],
                    'is_reply': False,
                    'parent_id': None
                }
                comments.append(comment_data)
                
                # Replies (nếu có)
                if 'replies' in item:
                    for reply in item['replies']['comments']:
                        reply_snippet = reply['snippet']
                        reply_data = {
                            'video_id': video_id,
                            'comment_id': reply['id'],
                            'author': reply_snippet['authorDisplayName'],
                            'text': reply_snippet['textDisplay'],
                            'like_count': reply_snippet['likeCount'],
                            'published_at': reply_snippet['publishedAt'],
                            'updated_at': reply_snippet['updatedAt'],
                            'is_reply': True,
                            'parent_id': item['snippet']['topLevelComment']['id']
                        }
                        comments.append(reply_data)
            
            # Lấy trang tiếp theo nếu có
            if 'nextPageToken' in response:
                request = youtube.commentThreads().list(
                    part='snippet,replies',
                    videoId=video_id,
                    maxResults=max_results,
                    pageToken=response['nextPageToken'],
                    textFormat='plainText'
                )
            else:
                break
                
    except Exception as e:
        print(f"Lỗi khi lấy comments cho video {video_id}: {str(e)}")
    
    return comments


if __name__ == "__main__":
    # Thực hiện crawl dữ liệu
    keyword = "cktg"
    print(f"Đang tìm kiếm video với từ khóa: {keyword}")
    
    # Bước 1: Search videos
    videos = search_videos(youtube, keyword, max_results=50)
    print(f"Tìm thấy {len(videos)} videos")
    print("\nDanh sách videos:")
    for i, video in enumerate(videos, 1):
        print(f"{i}. {video['title']} (ID: {video['video_id']})")
    
    # Bước 2: Crawl comments cho từng video
    all_comments = []
    
    for i, video in enumerate(videos, 1):
        print(f"\n[{i}/{len(videos)}] Đang lấy comments cho: {video['title']}")
        video_comments = get_video_comments(youtube, video['video_id'], max_results=100)
        all_comments.extend(video_comments)
        print(f"  → Đã lấy được {len(video_comments)} comments")
    
    print(f"\n{'='*60}")
    print(f"HOÀN THÀNH! Tổng số comments đã crawl: {len(all_comments)}")
    print(f"{'='*60}")
    
    # Tạo DataFrame để xem và lưu dữ liệu
    df_videos = pd.DataFrame(videos)
    df_comments = pd.DataFrame(all_comments)
    
    print("\nThông tin videos:")
    print(df_videos[['video_id', 'title', 'channel_title']])
    
    print("\nMẫu comments:")
    print(df_comments.head(10))
    
    # Lưu dữ liệu ra file CSV
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Lưu danh sách videos
    videos_filename = f'videos_{keyword.replace(" ", "_")}_{timestamp}.csv'
    df_videos.to_csv(videos_filename, index=False, encoding='utf-8-sig')
    print(f"\nĐã lưu danh sách videos vào: {videos_filename}")
    
    # Lưu comments
    comments_filename = f'comments_{keyword.replace(" ", "_")}_{timestamp}.csv'
    df_comments.to_csv(comments_filename, index=False, encoding='utf-8-sig')
    print(f"Đã lưu comments vào: {comments_filename}")
    
    print("\nThống kê:")
    print(f"- Tổng số videos: {len(df_videos)}")
    print(f"- Tổng số comments: {len(df_comments)}")
    print(f"- Số comments trả lời: {df_comments['is_reply'].sum()}")
    print(f"- Số comments gốc: {(~df_comments['is_reply']).sum()}")