import configparser
import os
import re
from functools import lru_cache
from datetime import datetime
import requests
from loguru import logger
from requests.adapters import HTTPAdapter
from tqdm import tqdm
import oss2  # 阿里云 OSS SDK
import mysql.connector
from utils import XBogusUtil
from utils import my_util

# OSS 连接信息 (根据你的实际 OSS 配置调整)
access_key_id = os.getenv('OSS_ACCESS_KEY_AIGC')
access_key_secret = os.getenv('OSS_ACCESS_KEY_SECRET_AIGC')
endpoint = os.getenv('OSS_ENDPOINT')
bucket_name = 'datas-aigc'

# 初始化 OSS bucket
bucket = oss2.Bucket(
    oss2.Auth(access_key_id, access_key_secret), endpoint, bucket_name
)

DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'root123',
    'database': 'external_data'
}

def connect_to_database():
    return mysql.connector.connect(
        host='127.0.0.1',
        user='root',
        password='root123',
        database='external_data',
        charset="utf8mb4"
    )


def insert_file_url(connection, uid, nickname, vid, desc_msg, online_time, oss_path, comment_count, digg_count,
                    collect_count, share_count, url):
    cursor = connection.cursor()
    try:
        insert_query = "INSERT INTO wangpan_douyin_video (uid, nickname, vid, desc_msg, online_time, oss_path, comment_count, digg_count, collect_count, share_count, url_path, status, create_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, now())"
        cursor.execute(insert_query, (
        uid, nickname, vid, desc_msg, online_time, oss_path, comment_count, digg_count, collect_count, share_count,
        url))
        connection.commit()
    finally:
        cursor.close()


def update_record(connection, record_id, status):
    cursor = connection.cursor()
    query = "UPDATE wangpan_douyin_video SET status = %s WHERE vid = %s"
    cursor.execute(query, (status, record_id))
    connection.commit()
    cursor.close()


def upload_to_oss(object_name, file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} does not exist.")
    auth = oss2.Auth(access_key_id, access_key_secret)
    bucket = oss2.Bucket(auth, endpoint, bucket_name)
    bucket.put_object_from_file(object_name, file_path)


def read_cookie_from_file():
    try:
        config = configparser.RawConfigParser()
        config.read('config.ini')
        con = dict(config.items('douyin'))
        if con is {}:
            raise Exception
        cookie = con.get('cookie')
        if cookie == '':
            logger.error('cookie值为空，请尝试手动填入cookie')
            raise Exception
    except Exception as e:
        logger.error(e)
        exit('请检查当前目录下的config.ini文件配置')
    return cookie


@lru_cache(maxsize=10)
def get_global_session():
    s = requests.Session()

    # 设置全局headers
    s.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
        'Referer': 'https://www.douyin.com/'
    })
    s.cookies.update({'Cookie': read_cookie_from_file()})
    s.mount('http://', HTTPAdapter(max_retries=3))
    s.mount('https://', HTTPAdapter(max_retries=3))
    return s


def analyze_user_input(user_in: str):
    try:
        u = re.search(r'user/([-\w]+)', user_in)
        if u:
            return u.group(1)
        u = re.search(r'https://v.douyin.com/(\w+)/', user_in)
        if u:
            url = u.group(0)
            res = get_global_session().get(url=url).url
            uid = re.search(r'user/([-\w]+)', res)
            if uid:
                return uid.group(1)

    except Exception as e:
        print(e)
        return


def crawl_media_scan(user_in: str, connection):
    # douyin不使用代理
    os.environ['NO_PROXY'] = 'douyin.com'
    video_list = []
    picture_list = []
    session = get_global_session()
    # 抖音用户唯一标识 sec_uid
    sec_uid = analyze_user_input(user_in)
    if sec_uid is None:
        exit("粘贴的用户主页地址格式错误")

    cursor = 0
    while 1:
        home_url = f'https://www.douyin.com/aweme/v1/web/aweme/post/?aid=6383&sec_user_id={sec_uid}&count=18&max_cursor={cursor}&cookie_enabled=true&platform=PC&downlink=6.9'
        xbs = XBogusUtil.generate_url_with_xbs(home_url, get_global_session().headers.get('User-Agent'))
        # 计算出X-Bogus参数拼接到url
        url = home_url + '&X-Bogus=' + xbs
        json_str = session.get(url).json()

        cursor = json_str["max_cursor"]  # 当页页码
        for i in json_str["aweme_list"]:
            #  视频收集
            if i["images"] is None:
                description = i["desc"]
                nickname = i["author"]["nickname"]
                uid = i["author"]["uid"]
                url = i["video"]["play_addr"]["url_list"][0]
                vid = str(i["aweme_id"])
                oss_path = 'oss://datas-aigc/aigc/short_play/10短剧内容库/大V/' + nickname + '/' + description
                comment_count = i["statistics"]["comment_count"]
                digg_count = i["statistics"]["digg_count"]
                collect_count = i["statistics"]["collect_count"]
                share_count = i["statistics"]["share_count"]
                online_time = datetime.fromtimestamp(i["create_time"]).strftime('%Y%m%d%H%M%S')

                video_list.append([description, url, nickname, vid])
                try:
                    insert_file_url(connection, uid, nickname, vid, description, online_time, oss_path, comment_count,
                                    digg_count, collect_count, share_count, url)
                except mysql.connector.IntegrityError as e:
                    continue
                except Exception as e:
                    print(e)
                    print("失败:" + description)
                    continue

            #  图片收集
            else:
                picture_list += list(map(lambda x: x["url_list"][-1], i["images"]))

        # 如果has_more为0说明已经到了尾页，结束爬取
        if json_str["has_more"] == 0:
            break
        # 随机睡眠
        my_util.random_sleep()


def crawl_media_down(user_in: str, connection, nickname: str):
    cursor = connection.cursor()
    # douyin不使用代理
    os.environ['NO_PROXY'] = 'douyin.com'
    video_list = []
    picture_list = []
    session = get_global_session()
    # 抖音用户唯一标识 sec_uid
    sec_uid = analyze_user_input(user_in)
    if sec_uid is None:
        exit("粘贴的用户主页地址格式错误")

    query = "SELECT desc_msg, url_path, nickname, vid FROM wangpan_douyin_video where status=0 and nickname=%s"
    cursor.execute(query, (nickname,))
    result = cursor.fetchall()
    for i in result:
        video_list.append([i[0], i[1], i[2], i[3]])

    download_media(session, sec_uid, video_list, picture_list)


def download_media(session: requests.Session, sec_uid, video_list, picture_list):
    print(sec_uid)
    if not os.path.exists(sec_uid):
        os.mkdir(sec_uid)
    os.chdir(sec_uid)

    with tqdm(total=len(video_list) + len(picture_list), desc="下载进度", unit="文件") as pbar:

        for i in video_list:
            des = i[0]
            url = i[1]
            nickname = i[2]
            vid = i[3]
            with session.get(url, stream=True) as response:
                if response.status_code == 200:
                    file_name = my_util.sanitize_filename(des)
                    local_file_path = f'{file_name}.mp4'
                    with open(local_file_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    oss_path = 'aigc/short_play/10短剧内容库/大V/' + nickname + '/' + des + '.mp4'
                    upload_to_oss(oss_path.strip(), local_file_path)
                    pbar.update(1)  # 完成当前文件的处理
                    update_record(connection, vid, 1)  # 成功时设置为1
                    # 删除本地文件
                    if os.path.exists(local_file_path):
                        os.remove(local_file_path)
                else:
                    print(f"网络异常 Status code: {response.status_code}")

        for i in picture_list:
            url = i
            with session.get(url, stream=True) as response:
                if response.status_code == 200:
                    file_name = my_util.IDGenerator.generate_unique_id()
                    with open(f'{file_name}.jpg', "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                else:
                    print(f"网络异常 Status code: {response.status_code}")
            pbar.update(1)  # 完成当前文件的处理

    print('用户视频图片已全部下载完成')
    os.chdir('..')


if __name__ == '__main__':
    try:
        connection = connect_to_database()
        user_map = {
            # '秦苒': 'https://www.douyin.com/user/MS4wLjABAAAANFgQxszykCn7A-QQb47sIUXx7mPIzZxAo0uA2ZpVt6FXkDkQtq4cPwuolK-ajRmq?from_tab_name=main',
            # '王七叶': 'https://www.douyin.com/user/MS4wLjABAAAAx9bJJ-j_53d3oTtZGZ5c1Eo2ZhGRIerp0QrsHK5Dc8I?from_tab_name=main',
            '林鸽': 'https://www.douyin.com/user/MS4wLjABAAAAZ3a-wZdXHkzmT7MHMGWwVbWze331dnRnjY2djIVYe4JN_wbsrnMCV8EE2aRNb_Ne?_sw=4046619020373895&from_tab_name=main',
            # '丁公子': 'https://www.douyin.com/user/MS4wLjABAAAAS3pOM-LyGmbfLKmpgKsiobmZUw9uHP5irTeVePR-y96YEwJyCuto3jBW5navVv4o?from_tab_name=main',
            # '莫邪': 'https://www.douyin.com/user/MS4wLjABAAAAdLun70v1eGwI6FuPoE7leS5_6hDfvPfXkxAq5ytwFkI?from_tab_name=main',
            # '姜十七': 'https://www.douyin.com/user/MS4wLjABAAAAjVocn5B2KaVZX7O3N4CJxPXlHAFVFkBpMIRs99SJ6KYQZnCsJ2L3LOFjvgj9xuaD?from_tab_name=main',
            # '祝晓晗': 'https://www.douyin.com/user/MS4wLjABAAAAm2w4lcbzh2wL9mgguS2aSk4v8qmMKCyq1K9zK0sx1dY?from_tab_name=main'
        }

        # 循环遍历字典，使用昵称和URL
        for nickname, url in user_map.items():
            crawl_media_scan(url, connection)
            crawl_media_down(url, connection, nickname)

    finally:
        connection.close()


DB_CONFIG = {
    'host': '10.100.5.125',
    'user': 'root',
    'password': 'YCph4YYcB3Ag',
    'database': 'external_data'
}

def connect_to_database():
    return mysql.connector.connect(
        host='10.100.5.125',
        user='root',
        password='YCph4YYcB3Ag',
        database='external_data',
        charset="utf8mb4"
    )