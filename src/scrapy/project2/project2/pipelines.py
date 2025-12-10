import psycopg2
from scrapy.exceptions import DropItem
from .items import TmdbMovieItem


class PostgresPipeline:
    # ⚠️ 关键：新人物 ID 从 16490 开始
    START_PEOPLE_ID = 34527

    # 内部缓存，用于存储本次爬取中已处理的人物的 ID，减少数据库查询
    # 结构: {('surname', 'first_name'): assigned_id}
    people_cache = {}

    # 当前分配到的 ID 指针
    current_people_id = START_PEOPLE_ID

    # ----------------------------------------------------------------
    # 数据库连接和初始化
    # ----------------------------------------------------------------

    @classmethod
    def from_crawler(cls, crawler):
        # 从 settings.py 中加载数据库配置
        db_settings = crawler.settings.getdict('DATABASE')
        return cls(db_settings)

    def __init__(self, db_settings):
        self.db_settings = db_settings
        self.conn = None
        self.cursor = None

    def open_spider(self, spider):
        """爬虫开启时连接数据库"""
        try:
            self.conn = psycopg2.connect(
                host=self.db_settings['host'],
                port=self.db_settings['port'],
                database=self.db_settings['database'],
                user=self.db_settings['username'],
                password=self.db_settings['password']
            )
            # 关闭自动提交，手动控制事务
            self.conn.autocommit = False
            self.cursor = self.conn.cursor()
            spider.logger.info("Database connection established successfully.")

            # 🌟 调整数据库序列：防止手动插入的 ID 与序列冲突
            # 将序列的当前值设置为 START_PEOPLE_ID - 1
            try:
                self.cursor.execute(f"SELECT setval('people_peopleid_seq', {self.START_PEOPLE_ID - 1}, true);")
                self.conn.commit()
                spider.logger.info(f"peopleid sequence set to start at {self.START_PEOPLE_ID}.")
            except psycopg2.Error as e:
                self.conn.rollback()
                spider.logger.warning(f"Could not set sequence (might not exist): {e}")

        except psycopg2.Error as e:
            spider.logger.error(f"Database connection failed: {e}")
            raise

    def close_spider(self, spider):
        """爬虫关闭时关闭连接"""
        if self.conn:
            self.conn.close()
            spider.logger.info("Database connection closed.")

    # ----------------------------------------------------------------
    # SQL 工具方法
    # ----------------------------------------------------------------

    def _execute_sql(self, sql, params=None):
        """执行 SQL 语句，出错时抛出异常由 process_item 捕获"""
        self.cursor.execute(sql, params)

    # ----------------------------------------------------------------
    # Item 处理逻辑
    # ----------------------------------------------------------------

    def process_item(self, item, spider):
        if not isinstance(item, TmdbMovieItem):
            return item

        try:
            # 1. 插入或更新 Movies 表 (父表)
            self._insert_movie(item)

            # 2. 遍历演职员，处理 People 和 Credits
            for person_data in item.get('cast_crew', []):
                # 获取有效的人物 ID (查找现有或插入新人物)
                people_id = self._insert_or_lookup_person(person_data)

                # 3. 插入 Credits 表 (子表)
                self._insert_credit(item['movieid'], people_id, person_data['job'])

            # 4. 提交整个事务 (只有当所有步骤都成功时)
            self.conn.commit()
            # spider.logger.debug(f"Committed transaction for movie: {item['movieid']}")

        except psycopg2.Error as e:
            self.conn.rollback()
            spider.logger.error(
                f"DB Error processing movie {item.get('movieid')}: {e.pgerror.strip() if e.pgerror else e}")
            # 不抛出 DropItem，以免中断爬虫，只记录错误并跳过当前 Item

        except Exception as e:
            self.conn.rollback()
            spider.logger.error(f"General Error processing movie {item.get('movieid')}: {e}")

        return item

    # ----------------------------------------------------------------
    # 独立插入方法
    # ----------------------------------------------------------------

    def _insert_movie(self, item):
        """插入或更新 Movies 表"""

        country_code = item['country']

        # 🌟 需求：将 'es' 替换为 'sp'
        if country_code == 'es':
            country_code = 'sp'

        # 注意：这里不再排除未知国家。如果 country_code 不在 countries 表中，
        # execute_sql 将抛出外键错误，这是符合预期的行为。

        sql = """
              INSERT INTO movies (movieid, title, country, year_released, runtime)
              VALUES (%s, %s, %s, %s, %s) ON CONFLICT (movieid) DO \
              UPDATE \
                  SET title = EXCLUDED.title, runtime = EXCLUDED.runtime; \
              """
        params = (
            item['movieid'],
            item['title'],
            country_code,
            int(item['release_date'][:4]) if item['release_date'] else 0,
            item['runtime']
        )
        self._execute_sql(sql, params)

    def _insert_or_lookup_person(self, person_data):
        """
        查找或插入人物，并返回 peopleid。
        逻辑：
        1. 检查本地缓存。
        2. 检查数据库 (SELECT)。
        3. 如果不存在，插入新记录 (INSERT)。
        """
        full_name = person_data.get('name')

        # 1. 拆分姓名 (surname NOT NULL, first_name NULLABLE)
        parts = full_name.strip().split(' ', 1)
        first_name = parts[0] if len(parts) > 1 else None
        surname = parts[-1]
        if len(parts) == 1:
            # 如果只有一个名字，视为 surname (根据您的表定义，surname 是必须的)
            first_name = None

        people_key = (surname, first_name)

        # 2. 检查本次运行缓存 (避免重复数据库查询)
        if people_key in self.people_cache:
            return self.people_cache[people_key]

        # 3. 检查数据库中是否已存在
        lookup_sql = """
                     SELECT peopleid \
                     FROM people
                     WHERE surname = %s \
                       AND first_name IS NOT DISTINCT \
                     FROM %s; \
                     """
        self._execute_sql(lookup_sql, (surname, first_name))
        result = self.cursor.fetchone()

        if result:
            # 如果数据库中有，使用数据库的 ID
            people_id = result[0]
            self.people_cache[people_key] = people_id
            return people_id

        # 4. 人物不存在，使用新分配的 ID 插入
        assigned_id = self.current_people_id

        born_year = person_data.get('born', 0)
        died_year = person_data.get('died')  # 如果是 None，插入为 NULL
        gender_char = person_data.get('gender', '?')

        insert_sql = """
                     INSERT INTO people (peopleid, first_name, surname, born, died, gender)
                     VALUES (%s, %s, %s, %s, %s, %s); \
                     """
        params = (
            assigned_id,
            first_name,
            surname,
            born_year,
            died_year,
            gender_char
        )
        self._execute_sql(insert_sql, params)

        # 插入成功后，更新指针和缓存
        self.current_people_id += 1
        self.people_cache[people_key] = assigned_id

        return assigned_id

    def _insert_credit(self, movieid, peopleid, credited_as):
        """插入 Credits 表"""
        sql = """
              INSERT INTO credits (movieid, peopleid, credited_as)
              VALUES (%s, %s, %s) ON CONFLICT DO NOTHING; \
              """
        params = (movieid, peopleid, credited_as)
        self._execute_sql(sql, params)