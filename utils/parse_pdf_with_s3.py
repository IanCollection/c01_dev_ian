import requests
import os
import json
import time
import base64
import boto3
from io import BytesIO
from urllib.parse import urlparse, quote

def parse_pdf(pdf_path: str) -> dict:
    """
    调用API解析PDF文件，支持本地文件路径或S3 URL

    Args:
        pdf_path: PDF文件路径或S3 URL

    Returns:
        dict: API返回的解析结果
    """
    start_time = time.time()
    base_url = "http://106.14.88.25:8000/pdf_parse"

    try:
        # 准备请求参数
        params = {
            "return_content_list": True,
            "is_json_md_dump": True,
            "return_layout": True,
            "return_info": True,
            "return_images": True
        }
        
        # 检查是否是S3 URL
        if pdf_path.startswith('s3://'):

            # 将S3路径添加到参数中
            params["pdf_path"] = pdf_path
            
            # 获取文件名（不含扩展名）用于保存结果
            # parsed_url = urlparse(pdf_path)
            # object_key = parsed_url.path.lstrip('/')
            # # file_name = os.path.splitext(os.path.basename(object_key))[0]
            #
            # 构建完整URL
            url = f"{base_url}?return_content_list=true&is_json_md_dump=true&return_layout=true&return_info=true&return_images=true&pdf_path={quote(pdf_path)}"

            payload = {}
            files = {}
            headers = {}
            response = requests.request("POST", url, headers=headers, data=payload, files=files)
        else:
            # 本地文件处理
            if not os.path.exists(pdf_path):
                return {"error": f"文件不存在: {pdf_path}"}
                
            # 获取文件名（不含扩展名）
            # file_name = os.path.splitext(os.path.basename(pdf_path))[0]
            
            # 打开PDF文件
            with open(pdf_path, 'rb') as f:
                files = {'pdf_file': f}
                # 发送请求
                print(f"发送请求到API: {base_url}")
                response = requests.post(base_url, files=files, params=params, timeout=300)
        
        # 检查响应状态
        if response.status_code == 200:
            response_data = response.json()

            # 创建结果目录
            # result_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "result_data_v3")
            # os.makedirs(result_dir, exist_ok=True)

            # 保存content_list到单独的JSON文件
            # if 'content_list' in response_data and response_data['content_list']:
            #     content_list_json_path = os.path.join(result_dir, f"{file_name}_content_list.json")
            #     with open(content_list_json_path, "w", encoding="utf-8") as f:
            #         json.dump(response_data['content_list'], f, ensure_ascii=False, indent=2)
            #     print(f"内容列表已保存到: {content_list_json_path}")

            # 保存图片数据到单独的JSON文件
            # if 'images' in response_data and response_data['images']:
            #     images_json_path = os.path.join(result_dir, f"{file_name}_images.json")
            #     with open(images_json_path, "w", encoding="utf-8") as f:
            #         json.dump(response_data['images'], f, ensure_ascii=False, indent=2)
            #     print(f"图片数据已保存到: {images_json_path}")

            # 保存md_content到单独的JSON文件
            # if 'md_content' in response_data and response_data['md_content']:
            #     md_content_json_path = os.path.join(result_dir, f"{file_name}_md_content.json")
            #     with open(md_content_json_path, "w", encoding="utf-8") as f:
            #         json.dump(response_data['md_content'], f, ensure_ascii=False, indent=2)
            #     print(f"Markdown内容已保存到: {md_content_json_path}")

            end_time = time.time()
            process_time = end_time - start_time
            print(f"PDF解析完成，总耗时: {process_time:.2f}秒")
            return response_data
        else:
            end_time = time.time()
            process_time = end_time - start_time
            print(f"API请求失败，总耗时: {process_time:.2f}秒")
            return {"error": f"API请求失败，状态码: {response.status_code}"}
    except requests.exceptions.Timeout:
        end_time = time.time()
        process_time = end_time - start_time
        print(f"请求超时，总耗时: {process_time:.2f}秒")
        return {"error": "请求超时，API服务器未能及时响应"}
    except requests.exceptions.ConnectionError:
        end_time = time.time()
        process_time = end_time - start_time
        print(f"连接错误，总耗时: {process_time:.2f}秒")
        return {"error": "连接错误，无法连接到API服务器"}
    except Exception as e:
        end_time = time.time()
        process_time = end_time - start_time
        print(f"解析错误，总耗时: {process_time:.2f}秒")
        return {"error": f"解析PDF时发生错误: {str(e)}"}

if __name__ == "__main__":
    s3_path = "s3://dj-storage/dj-docs/5fa9e38eec280217bad1665c2e847255811fb23f.pdf"
    parse_pdf(s3_path)
