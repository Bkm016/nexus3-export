import asyncio
import aiohttp
import json
import os
import logging
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin
import time
from tqdm import tqdm

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='nexus_export.log'
)

# Nexus配置
NEXUS_URL = "http://localhost:8081"
USERNAME = "admin"
PASSWORD = "admin123"
BASE_OUTPUT_DIR = "nexus_artifacts"  # 基础输出目录
MAX_CONCURRENT_DOWNLOADS = 10

class NexusExporter:
    def __init__(self):
        self.session = None
        self.auth = aiohttp.BasicAuth(USERNAME, PASSWORD)
        self.executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DOWNLOADS)
        self.stats = {}  # 每个仓库的统计信息
        
    async def create_session(self):
        self.session = aiohttp.ClientSession(auth=self.auth)

    async def close_session(self):
        if self.session:
            await self.session.close()

    async def get_repositories(self):
        """获取所有仓库列表"""
        try:
            async with self.session.get(f"{NEXUS_URL}/service/rest/v1/repositories") as response:
                if response.status == 200:
                    return await response.json()
                else:
                    error_text = await response.text()
                    logging.error(f"Failed to get repositories: Status {response.status}, Response: {error_text}")
                    raise Exception(f"Failed to get repositories: Status {response.status}")
        except Exception as e:
            logging.error(f"Error getting repositories: {str(e)}")
            raise

    async def get_components(self, repository_name, continuation_token=None):
        """获取仓库中的组件"""
        try:
            params = {"repository": repository_name}
            if continuation_token:
                params["continuationToken"] = continuation_token

            async with self.session.get(
                f"{NEXUS_URL}/service/rest/v1/components",
                params=params
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    error_text = await response.text()
                    logging.error(f"Failed to get components: Status {response.status}, Response: {error_text}")
                    return {"items": [], "continuationToken": None}
        except Exception as e:
            logging.error(f"Error getting components: {str(e)}")
            return {"items": [], "continuationToken": None}

    async def download_asset(self, asset, repository_name):
        """下载单个资产"""
        try:
            download_url = asset["downloadUrl"]
            relative_path = asset["path"]
            # 在仓库自己的目录下保持原有的路径结构
            full_path = os.path.join(BASE_OUTPUT_DIR, repository_name, relative_path)

            # 初始化仓库统计信息
            if repository_name not in self.stats:
                self.stats[repository_name] = {
                    "downloaded_assets": 0,
                    "downloaded_size": 0,
                    "start_time": time.time()
                }

            # 如果文件已存在且大小正确，跳过下载
            if os.path.exists(full_path):
                if "fileSize" in asset and os.path.getsize(full_path) == asset["fileSize"]:
                    print(f"[{repository_name}] Skipping existing file: {relative_path}")
                    return

            os.makedirs(os.path.dirname(full_path), exist_ok=True)

            async with self.session.get(download_url) as response:
                if response.status == 200:
                    with open(full_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(8192):
                            f.write(chunk)
                            self.stats[repository_name]["downloaded_size"] += len(chunk)
                    
                    self.stats[repository_name]["downloaded_assets"] += 1
                    print(f"[{repository_name}] Downloaded ({self.stats[repository_name]['downloaded_assets']}): {relative_path}")
                    logging.info(f"Successfully downloaded: {repository_name}/{relative_path}")
                else:
                    logging.error(f"Failed to download {repository_name}/{relative_path}: {response.status}")
        except Exception as e:
            logging.error(f"Error downloading {repository_name}/{relative_path}: {str(e)}")

    async def process_repository(self, repository):
        """处理单个仓库"""
        try:
            repository_name = repository["name"]
            print(f"\nProcessing repository: {repository_name}")
            logging.info(f"Processing repository: {repository_name}")

            # 创建仓库专属目录
            os.makedirs(os.path.join(BASE_OUTPUT_DIR, repository_name), exist_ok=True)

            continuation_token = None
            while True:
                result = await self.get_components(repository_name, continuation_token)
                
                download_tasks = []
                for item in result["items"]:
                    for asset in item["assets"]:
                        download_tasks.append(self.download_asset(asset, repository_name))

                if download_tasks:
                    await asyncio.gather(*download_tasks)

                continuation_token = result.get("continuationToken")
                if not continuation_token:
                    break

            # 打印仓库下载完成的统计信息
            if repository_name in self.stats:
                duration = time.time() - self.stats[repository_name]["start_time"]
                size_mb = self.stats[repository_name]["downloaded_size"] / (1024*1024)
                speed = size_mb / duration if duration > 0 else 0
                print(f"\nRepository {repository_name} completed:")
                print(f"Files downloaded: {self.stats[repository_name]['downloaded_assets']}")
                print(f"Total size: {size_mb:.2f} MB")
                print(f"Average speed: {speed:.2f} MB/s")

        except Exception as e:
            logging.error(f"Error processing repository {repository_name}: {str(e)}")

    async def export_all(self):
        """导出所有仓库的内容"""
        try:
            await self.create_session()
            os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

            try:
                repositories = await self.get_repositories()
                if not repositories:
                    print("No repositories found or failed to get repositories list.")
                    return

                # 串行处理仓库（一个接一个）
                for repo in repositories:
                    await self.process_repository(repo)

            except Exception as e:
                logging.error(f"Error in export_all: {str(e)}")
                print(f"Error occurred: {str(e)}")

        finally:
            await self.close_session()

async def main():
    start_time = time.time()
    
    print(f"Starting Nexus export from: {NEXUS_URL}")
    print(f"Base output directory: {BASE_OUTPUT_DIR}")
    
    exporter = NexusExporter()
    await exporter.export_all()
    
    end_time = time.time()
    duration = end_time - start_time
    
    print("\nOverall Export Summary:")
    total_files = sum(repo_stats["downloaded_assets"] for repo_stats in exporter.stats.values())
    total_size = sum(repo_stats["downloaded_size"] for repo_stats in exporter.stats.values())
    print(f"Total repositories processed: {len(exporter.stats)}")
    print(f"Total files downloaded: {total_files}")
    print(f"Total size downloaded: {total_size / (1024*1024):.2f} MB")
    print(f"Total time: {duration:.2f} seconds")
    if duration > 0:
        print(f"Overall average speed: {(total_size / duration) / (1024*1024):.2f} MB/s")
    
    logging.info(f"Export completed. Total execution time: {duration:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())
