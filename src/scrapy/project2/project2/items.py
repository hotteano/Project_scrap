# project2/items.py
import scrapy


class TmdbMovieItem(scrapy.Item):
    # Movies 表字段
    movieid = scrapy.Field()
    title = scrapy.Field()
    release_date = scrapy.Field()
    country = scrapy.Field()
    runtime = scrapy.Field()

    # 演职员列表 (用于更新 people 和 credits 表)
    cast_crew = scrapy.Field()