# tmdb_scraper/settings.py

# Scrapy settings for project2 project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
# https://docs.scrapy.org/en/latest/topics/settings.html

BOT_NAME = 'tmdb_scraper'  # 你的爬虫标识符

# 确保这里的路径指向你的 spiders 目录
SPIDER_MODULES = ['project2.spiders']
NEWSPIDER_MODULE = 'project2.spiders'


# ----------------------------------------------------
# 1. 爬虫身份识别 (防止被封禁)
# ----------------------------------------------------

# 建议使用你的邮箱或项目 URL，而不是默认的 Scrapy 字符串
USER_AGENT = 'tmdb_scraper (edwardchenyq@gmail.com)'

# 遵守 robots.txt 协议 (通常 TMDB API 不需要，但这是好习惯)
ROBOTSTXT_OBEY = True


# ----------------------------------------------------
# 2. 速率限制与并发 (遵守 TMDB API 限制)
# ----------------------------------------------------

# TMDB API 限制通常是 40 次请求/10 秒。以下配置是为了安全起见。
CONCURRENT_REQUESTS = 4  # 同时发起的最大请求数

# 下载延迟：每次请求间隔 0.25 秒，确保每秒不超过 4 个请求 (4 * 0.25 = 1秒)
DOWNLOAD_DELAY = 0.25

# 启用 AutoThrottle 扩展，根据服务器负载动态调整延迟
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 5.0      # 初始延迟时间
AUTOTHROTTLE_MAX_DELAY = 60.0       # 最大延迟时间
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0 # Scrapy 将尽量将并发请求数保持在 1.0/DOWNLOAD_DELAY


# ----------------------------------------------------
# 3. Pipeline 配置 (数据库存储)
# ----------------------------------------------------

DATABASE = {
    'drivername': 'postgresql',
    'host': 'localhost',        # 替换为您的数据库主机
    'port': '5432',
    'database': 'project2',  # 替换为您的数据库名
    'username': 'postgres', # 替换为您的用户名
    'password': '1234'  # 替换为您的密码
}

ITEM_PIPELINES = {
    'project2.pipelines.PostgresPipeline': 300,
}

# ----------------------------------------------------
# 4. 其他配置 (可选但推荐)
# ----------------------------------------------------

# 设置日志级别，通常在调试时设为 DEBUG，正式运行时设为 INFO
LOG_LEVEL = 'INFO'

# 设置重试次数
RETRY_TIMES = 3

# 禁用 cookies (API 爬取通常不需要)
COOKIES_ENABLED = False