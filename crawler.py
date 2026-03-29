import re
import json
import logging
from curl_cffi import requests as curl_requests
import requests  # 用于请求普通的开源 API
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class WHVCrawler:
    def __init__(self):
        self.url_462 = "https://immi.homeaffairs.gov.au/visas/getting-a-visa/visa-listing/work-holiday-462/specified-462-work"
        self.url_417 = "https://immi.homeaffairs.gov.au/visas/getting-a-visa/visa-listing/work-holiday-417/specified-work"

        self.final_data = {}

        # 新增：用于存储邮编和镇的映射
        self.postcode_to_towns = {}
        self.town_to_postcode = {}  # 反向索引，方便前端通过镇名查邮编

    def fetch_australia_postcodes(self):
        """拉取开源的澳洲邮编数据库，建立地名映射"""
        logging.info("📚 正在获取全澳邮编地名开源数据库...")
        # 这是一个著名的澳洲邮编开源库
        url = "https://raw.githubusercontent.com/matthewproctor/australianpostcodes/master/australian_postcodes.json"
        try:
            # 普通的 github raw 链接不需要防爬伪装
            response = requests.get(url, timeout=20)
            if response.status_code == 200:
                data = response.json()
                for item in data:
                    pc = str(item.get('postcode', '')).zfill(4)  # 补齐4位
                    town = str(item.get('locality', '')).title()  # 首字母大写
                    town_lower = town.lower()

                    if not pc or not town: continue

                    # 建立邮编 -> 镇名的映射
                    if pc not in self.postcode_to_towns:
                        self.postcode_to_towns[pc] = []
                    if town not in self.postcode_to_towns[pc]:
                        self.postcode_to_towns[pc].append(town)

                    # 建立镇名 -> 邮编的反向映射 (只保留最匹配的一个即可)
                    self.town_to_postcode[town_lower] = pc

                logging.info(f"✅ 成功加载了 {len(self.postcode_to_towns)} 个邮编的地理信息！")
            else:
                logging.error("获取地理数据库失败。")
        except Exception as e:
            logging.error(f"请求地理数据库异常: {e}")

    def fetch_html(self, url: str) -> str:
        """穿透 WAF 获取移民局网页源码"""
        logging.info(f"正在抓取: {url}")
        try:
            response = curl_requests.get(url, impersonate="chrome120", timeout=20)
            if response.status_code == 200:
                return response.text
        except Exception as e:
            logging.error(f"请求异常: {e}")
        return ""

    def parse_postcode_string(self, raw_string: str) -> list[str]:
        """解析并展平极度随意的邮编字符串"""
        clean_string = raw_string.replace('\xa0', ' ').replace('\n', ' ').strip().lower()
        parts = re.split(r'[,;]|\band\b', clean_string)
        result_set = set()

        for part in parts:
            part = part.strip()
            if not part: continue

            range_match = re.search(r'(\d{4})\s*(?:to|-|–|—)\s*(\d{4})', part)
            if range_match:
                start, end = int(range_match.group(1)), int(range_match.group(2))
                if start <= end:
                    for i in range(start, end + 1):
                        result_set.add(f"{i:04d}")
            else:
                single_matches = re.findall(r'\b\d{4}\b', part)
                for match in single_matches:
                    result_set.add(match)
        return sorted(list(result_set))

    def extract_data_from_html(self, html: str, visa_type: str):
        """破解 SharePoint 隐藏 JSON 数据机制，提取表格（彻底修复严格过滤导致的漏抓）"""
        soup = BeautifulSoup(html, 'html.parser')
        input_tag = soup.find('input', {'id': 'ctl00_PlaceHolderMain_PageSchemaHiddenField_Input'})
        if not input_tag: return

        try:
            page_data = json.loads(input_tag.get('value', ''))
        except json.JSONDecodeError:
            return

        for section in page_data.get('content', []):
            section_title = section.get('text', '').lower()
            html_block = section.get('block', '')
            if not html_block: continue

            # 扩大关键词库，确保不遗漏任何有效表格
            industries = set()

            # 1. 行业明文匹配 (新增农业/肉厂等变体)
            if any(kw in section_title for kw in ['tourism', 'hospitality']): industries.add('旅游和酒店业')
            if any(kw in section_title for kw in ['plant', 'animal', 'farm', 'agri', 'meat']): industries.add(
                '动植物栽培 (含肉厂/农业)')
            if any(kw in section_title for kw in ['tree', 'forest', 'fell']): industries.add('林业')
            if any(kw in section_title for kw in ['fish', 'pearl']): industries.add('捕鱼与采珠业')
            if 'mining' in section_title: industries.add('采矿业')
            if 'construction' in section_title: industries.add('建筑业')
            if any(kw in section_title for kw in ['bushfire', 'disaster', 'flood', 'recovery']): industries.add(
                '灾后重建')

            # 2. 宽泛地域兜底 (官方喜欢用地域命名面板)
            if 'northern' in section_title:
                industries.update(['旅游和酒店业', '动植物栽培 (含肉厂/农业)', '林业', '捕鱼与采珠业', '建筑业'])
            elif 'remote' in section_title:
                industries.update(['旅游和酒店业'])
            elif 'regional' in section_title:
                industries.update(['动植物栽培 (含肉厂/农业)', '建筑业', '林业', '采矿业'])

            # 3. 终极防漏盾牌：如果什么都没匹配到，但这个块里有包含数字的表格
            if not industries and 'table' in html_block:
                industries.update(['动植物栽培 (含肉厂/农业)', '建筑业'])  # 默认给范围最大的基础集签行业

            industries_list = list(industries)

            block_soup = BeautifulSoup(html_block, 'html.parser')
            for table in block_soup.find_all('table'):
                for row in table.find_all('tr'):
                    cols = row.find_all(['td', 'th'])
                    if len(cols) >= 2:
                        state = cols[0].get_text(strip=True)
                        raw_postcodes = cols[1].get_text(strip=True)

                        # 跳过表头
                        if 'postcode' in raw_postcodes.lower() or 'state' in state.lower(): continue

                        flattened_postcodes = self.parse_postcode_string(raw_postcodes)
                        if flattened_postcodes:
                            for ind in industries_list:
                                self.merge_into_final_data(flattened_postcodes, state, ind, visa_type)

    def merge_into_final_data(self, postcodes: list[str], state: str, industry: str, visa_type: str):
        """合并数据，并在此处注入镇名！"""
        for code in postcodes:
            if code not in self.final_data:
                # 查字典，获取这个邮编下的所有镇名，如果没有则为空列表
                towns = self.postcode_to_towns.get(code, [])

                self.final_data[code] = {
                    "state": state,
                    "towns": towns,  # 注入镇名数据！
                    "462": {"eligible": False, "industries": []},
                    "417": {"eligible": False, "industries": []}
                }

            self.final_data[code][visa_type]["eligible"] = True
            if industry not in self.final_data[code][visa_type]["industries"]:
                self.final_data[code][visa_type]["industries"].append(industry)

    def run(self):
        # 1. 先去获取全澳字典
        self.fetch_australia_postcodes()

        logging.info("🚀 启动 462 签证规则抓取...")
        html_462 = self.fetch_html(self.url_462)
        if html_462: self.extract_data_from_html(html_462, "462")

        logging.info("🚀 启动 417 签证规则抓取...")
        html_417 = self.fetch_html(self.url_417)
        if html_417: self.extract_data_from_html(html_417, "417")

        # ==========================================
        # 🛡️ 核心新增：数据熔断与安全校验机制
        # ==========================================
        total_postcodes = len(self.final_data)
        logging.info(f"📊 本次共解析出 {total_postcodes} 个包含集签规则的邮编。")

        # 正常情况下，澳洲符合集签的邮编应该在 1000 到 3000 个之间
        # 如果少于这个阈值，说明爬虫被 WAF 盾了，或者网页结构大改
        if total_postcodes < 500:
            error_msg = f"❌ 严重错误：抓取到的集签邮编数量异常偏少（仅 {total_postcodes} 个）！触发熔断机制，取消本次数据覆盖。"
            logging.error(error_msg)
            # 主动抛出异常，这会让 GitHub Actions 标记本次运行失败，并且不会执行后续的 git push
            raise Exception(error_msg)
        # ==========================================

        os.makedirs('data', exist_ok=True)

        output_data = {
            "update_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "postcode_data": self.final_data,
            "town_index": self.town_to_postcode
        }

        with open('data/rules.json', 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)

        logging.info("🎉 抓取完成！数据完整性校验通过，已存入 data/rules.json")


if __name__ == "__main__":
    crawler = WHVCrawler()
    crawler.run()