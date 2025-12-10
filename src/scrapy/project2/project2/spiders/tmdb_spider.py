# project2/spiders/tmdb_spider.py
import scrapy
import re


class TmdbSpider(scrapy.Spider):
    name = "tmdb"
    allowed_domains = ["api.themoviedb.org"]

    # ⚠️ 替换为您的真实 TMDB API KEY
    API_KEY = "c1f16eb601d7f58fbbb7eff4ac2c26f0"

    def start_requests(self):
        url = "https://api.themoviedb.org/3/discover/movie"
        base_params = {
            'api_key': self.API_KEY,
            'sort_by': 'popularity.desc',
            'page': 1,
            'with_release_type': '2|3',
        }

        # 策略：按年份遍历，每年作为一个独立的查询队列
        # 这样每年都能爬取最多 10,000 部电影，总数就是 N年 x 10000
        years = range(2025, 2026)  # 2019 到 2025

        for year in years:
            # 构造特定年份的查询参数
            year_params = base_params.copy()
            year_params['primary_release_year'] = year

            # 手动拼接 URL 参数
            query_string = '&'.join([f"{k}={v}" for k, v in year_params.items()])
            full_url = f"{url}?{query_string}"

            self.logger.info(f"Starting crawl for year: {year}")
            yield scrapy.Request(url=full_url, callback=self.parse_discover)

    def parse_discover(self, response):
        """处理 Discover API 的响应，提取电影ID并处理分页。"""
        try:
            data = response.json()
        except Exception as e:
            self.logger.error(f"Failed to parse JSON from Discover: {e}")
            return

        for movie in data.get('results', []):
            movie_id = movie.get('id')
            detail_url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={self.API_KEY}"
            yield scrapy.Request(
                url=detail_url,
                callback=self.parse_details,
                meta={'initial_data': movie}
            )

        current_page = data.get('page')
        total_pages = data.get('total_pages')

        if current_page is not None and current_page < total_pages:
            # 建议添加一个保护，防止超过 API 允许的最大页数 (通常是 500)
            if current_page >= 500:
                self.logger.warning("Reached TMDB API limit (Page 500). Stopping pagination for this query.")
            else:
                next_page = current_page + 1
                next_url = re.sub(r'page=\d+', f'page={next_page}', response.url)
                self.logger.info(f"Paging: Requesting page {next_page} of {total_pages}")
                yield scrapy.Request(url=next_url, callback=self.parse_discover)

    def parse_details(self, response):
        """解析详情页，并请求 Credits API。"""
        initial_data = response.meta['initial_data']
        try:
            detail_data = response.json()
        except Exception as e:
            self.logger.error(f"Failed to parse JSON from Details: {e}")
            return

        item_data = {
            'movieid': detail_data.get('id'),
            'title': initial_data.get('title'),
            'release_date': initial_data.get('release_date'),
            'runtime': detail_data.get('runtime', 0) or 0
        }

        # 处理国家代码 (转小写，若无则设为 ??)
        countries = detail_data.get('production_countries', [])
        if countries:
            code = countries[0].get('iso_3166_1')
            if code == 'ES' or code == 'es':
                code = 'sp'
            if code == 'KN' or code == 'kn':
                code = 'ke'
            item_data['country'] = code.lower() if code else '??'
        else:
            item_data['country'] = '??'

        # 请求 Credits
        credits_url = f"https://api.themoviedb.org/3/movie/{item_data['movieid']}/credits?api_key={self.API_KEY}"
        yield scrapy.Request(
            url=credits_url,
            callback=self.parse_credits,
            meta={'item_data': item_data}
        )

    def parse_credits(self, response):
        """处理 Credits API 的响应，并开始链式请求人物详情。"""
        from ..items import TmdbMovieItem

        item_data = response.meta['item_data']
        try:
            credits_data = response.json()
        except Exception as e:
            self.logger.error(f"Failed to parse JSON from Credits: {e}")
            return

        item = TmdbMovieItem(**item_data)
        processed_people = []

        # 1. 提取前 5 名演员 (Job='A')
        for p in credits_data.get('cast', [])[:5]:
            processed_people.append({
                'tmdb_id': p.get('id'),
                'name': p.get('name'),
                'job': 'A',
                'gender': None,
                'born': None,
                'died': None
            })

        # 2. 提取导演 (Job='D')
        for p in credits_data.get('crew', []):
            if p.get('job') == 'Director':
                processed_people.append({
                    'tmdb_id': p.get('id'),
                    'name': p.get('name'),
                    'job': 'D',
                    'gender': None,
                    'born': None,
                    'died': None
                })

        # 3. 开始链式请求人物详情
        if processed_people:
            item['cast_crew'] = processed_people

            first_person = processed_people[0]
            person_url = f"https://api.themoviedb.org/3/person/{first_person['tmdb_id']}?api_key={self.API_KEY}"

            yield scrapy.Request(
                url=person_url,
                callback=self.parse_person_details,
                meta={
                    'item': item,
                    'people_list': processed_people,
                    'current_index': 0
                }
            )
        else:
            item['cast_crew'] = []
            yield item

    def parse_person_details(self, response):
        """处理人物详情 API 的响应，更新 Item，并请求下一个人物。"""
        meta = response.meta
        try:
            person_data = response.json()
        except Exception as e:
            self.logger.error(f"Failed to parse JSON from Person: {e}")
            return

        item = meta['item']
        people_list = meta['people_list']
        current_index = meta['current_index']

        person = people_list[current_index]

        # 1. 提取性别 (Gender)
        # TMDB Gender: 1=女性(F), 2=男性(M), 其他=?
        gender_code = person_data.get('gender')
        if gender_code == 1:
            person['gender'] = 'F'
        elif gender_code == 2:
            person['gender'] = 'M'
        else:
            person['gender'] = '?'

        # 2. 提取出生年份 (born)
        birthday = person_data.get('birthday')
        if birthday:
            person['born'] = int(birthday[:4])
        else:
            person['born'] = 0  # NOT NULL 占位符

        # 3. 提取死亡年份 (died)
        deathday = person_data.get('deathday')
        if deathday:
            person['died'] = int(deathday[:4])
        else:
            person['died'] = None  # Python None 对应 SQL NULL

        # 4. 检查是否还有下一个人物需要处理
        next_index = current_index + 1

        if next_index < len(people_list):
            next_person = people_list[next_index]
            person_url = f"https://api.themoviedb.org/3/person/{next_person['tmdb_id']}?api_key={self.API_KEY}"

            yield scrapy.Request(
                url=person_url,
                callback=self.parse_person_details,
                meta={
                    'item': item,
                    'people_list': people_list,
                    'current_index': next_index
                }
            )
        else:
            # 所有人物处理完毕，yield 最终 Item
            yield item