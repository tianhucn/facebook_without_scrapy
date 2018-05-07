# -*- coding: utf-8 -*-
import hashlib
import cv2
import time
import numpy as np
import requests
import re
import json
import psutil
import asyncio

from bs4 import BeautifulSoup
from pyppeteer import launch
from io import BytesIO
from urllib.parse import urljoin, unquote
from datetime import datetime, date
from random import choice

FACEBOOK = 2
MAX_ACCOUNT = 100
COUNT = 50
GET_ACCOUNT = "http://root.wgclick.com:88/data/cgi/crawler/getResourceAccount"
REPORT_DATA = "http://root.wgclick.com:88/data/cgi/crawler/reportData"
IMAGE_UPLOAD = "http://picture.wgclick.com:80"
# proxies = {"http": "http://127.0.0.1:1080", "https": "https://127.0.0.1:1080", }
proxies = None
login_accounts = [("0971764790", "038441356"), ("0971784574", "038441185")]

browser = None


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(login())
    accounts = get_accounts(FACEBOOK)
    acct_list = [account["username"] for account in accounts]
    with open("record.txt", "a") as f:
        f.write("{}: Got {} Accounts. Account list: {} \n".format(now(), len(accounts), acct_list))
    if not accounts:
        print('No Facebook account')
        return
    print("{}: Got {} Facebook accounts".format(now(), len(accounts)))
    for account in accounts:
        last_publish_time = None
        if account["last_publish_time"] and account["last_publish_time"] != "0":
            last_publish_time = datetime.fromtimestamp(
                int(account["last_publish_time"])
            )
        username = str(account["username"]).strip()
        this_year = datetime.now().year
        asyncio.get_event_loop().run_until_complete(
            content_handler(username, this_year, last_publish_time, account)
        )
    asyncio.get_event_loop().run_until_complete(close_browser())


def kill_chrome():
    pids = psutil.pids()
    try:
        for pid in pids:
            p = psutil.Process(pid)
            if 'chrome' in p.name():
                p.kill()
                print('{}: Killed chrome process ({}: {})'.format(now(), p.pid, p.name()))
    except Exception as e0:
        print('{}: Killed chrome process error: {}'.format(now(), e0))


def now():
    return str(datetime.now())


async def close_browser():
    global browser
    await browser.close()
    print("{}： Close Pyppeteer browser".format(now()))


async def login():
    username, password = choice(login_accounts)
    print("{}: {} is logging...".format(now(), username))
    global browser
    browser = await launch(
        headless=True,
        args=["--no-sandbox"],
        handleSIGINT=False,
        handleSIGTERM=False,
        handleSIGHUP=False,
    )
    page = await browser.newPage()
    cookie = {
        "domain": ".facebook.com",
        "httponly": False,
        "name": "locale",
        "path": "/",
        "secure": True,
        "value": "zh_CN",
    }
    await page.setCookie(cookie)
    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 5.0; SM-G900P Build/LRX21T) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/66.0.3359.117 Mobile Safari/537.36"
    }
    await page.setExtraHTTPHeaders(headers)
    await page.goto("https://m.facebook.com/login")
    asyncio.sleep(1)
    await page.type("#m_login_email", username)
    await page.type("#m_login_password", password)
    await page.click("[name=login]")
    print("{}: Done".format(now()))


async def content_handler(username, year, last_publish_time, account):
    # browser = await launch(headless=False, args=['--no-sandbox', '--proxy-server=127.0.0.1:1080'],
    #                        handleSIGINT=False,
    #                        handleSIGTERM=False,
    #                        handleSIGHUP=False)
    global browser
    page = await browser.newPage()
    cookie = {
        "domain": ".facebook.com",
        "httponly": False,
        "name": "locale",
        "path": "/",
        "secure": True,
        "value": "zh_CN",
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 5.0; SM-G900P Build/LRX21T) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/66.0.3359.117 Mobile Safari/537.36"
    }
    await page.setCookie(cookie)
    await page.setExtraHTTPHeaders(headers)
    await page.goto("https://m.facebook.com/{}/year/{}/".format(username, year))
    for _ in range(5):
        await page.evaluate(
            """() => {
            window.scrollTo(0,document.body.scrollHeight);
        }"""
        )
        for _ in range(500):
            await page.keyboard.press("ArrowDown")
        await asyncio.sleep(1)
    html = await page.content()
    await page.close()
    parse(html, last_publish_time, account)


def parse(html, last_publish_time, account):
    soup = BeautifulSoup(html, "lxml")
    article = soup.select("article")
    count = COUNT
    for art in article:
        content = ""
        date_time = ""
        images = set()
        story = art.select("div.story_body_container")
        for each in story:
            p = each.find_all("p")
            for item in p:
                content += item.get_text()
            i = each.find_all("i")
            for item in i:
                url = get_image_url(item)
                if url:
                    images.add(url)
            abbr = each.select_one("abbr")
            date_time = parse_date(abbr.get_text())
        data_store = art.get("data-store")
        tweet_id = json.loads(data_store)["share_id"]
        item = dict()
        item["resource_account_id"] = account["id"]  # 这个ID是Java里账号的主键
        item["text"] = content
        item["publish_time"] = date_time
        item["crawler_time"] = int(time.time())
        item["hash"] = gen_hash(tweet_id, account["source_id"])
        item["image"] = [image for image in images]
        publish_time = date_time
        if publish_time and last_publish_time:
            if publish_time <= last_publish_time:
                print("{}: {} is up-to-date, no need to crawl".format(now(), account["username"]))
                return
        else:
            print("{}: First crawling. Count:{}".format(now(), count))
            if count == 0:
                print("{}: {} has crawled max value items".format(now(), account["username"]))
                return
            count -= 1
        report(item)


def receive_data():
    """
    从接口获取账号数据

    :return:
    """
    param = {"size": MAX_ACCOUNT, "model": 1}
    r = requests.get(GET_ACCOUNT, params=param)
    if r.status_code == 200:
        r = json.loads(r.text)
        is_successful = r.get("result")
        if is_successful:
            data = r.get("list")
            if data:
                print("{}: Got accounts from interface, max: {}".format(now(), MAX_ACCOUNT))
                return data
            else:
                print("{}: no account today".format(now()))
                return None
    print("{}: Failed to get accounts from interface".format(now()))
    return None


def get_accounts(source_id):
    """
    得到指定类型的账号

    :param source_id: 账号类型ID
                      1: 微博
                      2: FaceBook
                      3: Ins
                      4: Twitter
    :return:
    """
    accounts = receive_data()
    if accounts:
        return [c for c in accounts if int(c["source_id"]) == source_id]
    return None


def report_data(data_list):
    """
    上报数据

    :param data_list:  数据列表
    :return:
    """
    req = requests.post(REPORT_DATA, json=data_list)
    if req.status_code == 200:
        r = json.loads(req.text)
        if r.get("result"):
            print("{}: Result from interface: {}".format(now(), r.get("result")))
            print("{}: Reported an item".format(now()))
            return True
        else:
            print("{}: Reported abortively".format(now()))
            return False
    print("{}: Reporting interface invalid: {}".format(now(), req.status_code))
    return False


def save_img(item):
    urls = item["image"]
    md5_list = []
    for url in urls:
        info = item["hash"]
        file_name = info + ".jpg"
        req = requests.get(url, proxies=proxies)
        if req.status_code == 200:
            try:
                cropped = crop_10_percent(req.content)
                file = {(file_name, cropped.getvalue())}
                r = requests.post(urljoin(IMAGE_UPLOAD, "upload"), files=file)
                if r.status_code == 200:
                    result = json.loads(r.text)
                    result_info = result["info"]
                    md5 = result_info["md5"]
                    whole = urljoin(IMAGE_UPLOAD, md5)
                    if md5:
                        print("{}: Uploaded image: {}".format(now(), file_name))
                        md5_list.append(whole)
            except Exception as e1:
                print("{}: Uploaded image abortively: {}".format(now(), e1))
                return None
    return md5_list


def crop_10_percent(img):
    image = np.asarray(bytearray(img), dtype="uint8")
    image = cv2.imdecode(image, cv2.IMREAD_COLOR)
    height, width = image.shape[:2]
    new_height = height - height * 0.1
    cropped = image[0:int(new_height), 0:width]
    img_bytes = cv2.imencode(".jpg", cropped)[1]
    return BytesIO(img_bytes)


def report(item):
    # 将Datetime类型的时间转为时间戳
    item["publish_time"] = dt_2_ts(item["publish_time"])
    item["image"] = save_img(item)
    # 过滤Emoji表情
    plain_text = filter_emoji(item["text"])
    payload = [
        {
            "data": {
                "resource_account_id": item["resource_account_id"],
                "text": plain_text,
                "publish_time": item["publish_time"],
                "crawler_time": item["crawler_time"],
                "hash": item["hash"],
            },
            "image": item["image"],
        }
    ]
    report_data(payload)

    # 过滤掉内容里包含“分享视频的条目”
    keywords = ("分享视频", "视频链接", "秒拍视频")
    for k in keywords:
        if k in item["text"]:
            print("{}: Contain video keywords. Abandon: {}".format(now(), item["text"]))
            return
    return item


def dt_2_ts(dt):
    try:
        return int(time.mktime(dt.timetuple()))
    except Exception as e:
        print("{}: Datetime to Timestamp abortively：{}".format(now(), e))
        return None


def filter_emoji(text):
    """过滤Emoji表情"""
    highpoints_UCS4 = re.compile(u"[\U00010000-\U0010ffff]")
    highpoints_UCS2 = re.compile(u"[\uD800-\uDBFF][\uDC00-\uDFFF]")
    t1 = highpoints_UCS4.sub("", text)
    t2 = highpoints_UCS2.sub("", t1)
    return t2


def gen_hash(*args):
    """
    合并任意个对象并生成MD5值
    :param args:
    :return: hash
    """
    mix = ""
    for arg in args:
        if arg:
            mix += str(arg)
    md5 = hashlib.md5()
    md5.update(mix.encode("utf-8"))
    return md5.hexdigest()


def get_image_url(i):
    style = i.get("style")
    _class = i.get("class")
    if "profpic" in _class:
        return None
    if style is None:
        return None
    pattern = r"http.*?\)"
    ret = re.findall(pattern, style)
    if ret:
        url = ret[0][:-2]
        url = url.replace(" ", "")
        url = repr(url).replace(r"\\", "%")
        url = str(url)[1:-1]
        url = unquote(url)
        return url
    else:
        return None


def parse_date(date_str):
    pattern1 = r"(.*?)月(.*?)日"
    pattern2 = r"(.*?)年(.*?)月(.*?)日"
    ret = re.findall(pattern2, date_str)
    today = date.today()
    flag = False
    year, month, day = today.year, today.month, today.day
    if ret:
        year, month, day = ret[0]
        flag = True
    else:
        ret = re.findall(pattern1, date_str)
        if ret:
            month, day = ret[0]
            flag = True
    if flag:
        time = date_str.split(" ")[-1]
        hour, minute = time.split(":")
        date_time = datetime(int(year), int(month), int(day), int(hour), int(minute))
        return date_time
    else:
        return datetime.now()


def parse_comment(span):
    pattern = r"^[0-9]*"
    ret = re.findall(pattern, span)
    if ret:
        return ret[0]
    else:
        return 0


if __name__ == "__main__":
    while True:
        try:
            main()
            time.sleep(10)
        except Exception as e3:
            print("{}: Main loop Exception: {}".format(now(), e3))
