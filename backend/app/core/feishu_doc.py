#!/usr/bin/env python3
"""
飞书文档API客户端
用于调用飞书文档相关接口，特别是多维表格接口
"""

import requests
import json
from typing import Dict, Any, List


class FeishuDocClient:
    """
    飞书文档API客户端类
    """
    
    def __init__(self, app_id: str, app_secret: str):
        """
        初始化飞书文档API客户端
        
        Args:
            app_id: 飞书应用ID
            app_secret: 飞书应用密钥
        """
        self.app_id = app_id
        self.app_secret = app_secret
        self.base_url = "https://open.feishu.cn/open-apis"
        self.access_token = None
        
    def get_access_token(self) -> str:
        """
        获取访问令牌
        
        Returns:
            访问令牌字符串
        """
        if not self.access_token:
            url = f"{self.base_url}/auth/v3/tenant_access_token/internal/"
            headers = {
                "Content-Type": "application/json; charset=utf-8"
            }
            payload = {
                "app_id": self.app_id,
                "app_secret": self.app_secret
            }
            
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            response.raise_for_status()
            result = response.json()
            
            if result.get("code") != 0:
                raise Exception(f"获取访问令牌失败: {result.get('msg')}")
            
            self.access_token = result.get("tenant_access_token")
        
        return self.access_token
    
    def get_spreadsheet_records(self, app_token: str, table_id: str = None, filter_params: Dict[str, Any] = None, page_size: int = 100) -> List[Dict[str, Any]]:
        """
        获取多维表格的记录（使用search接口）
        
        Args:
            app_token: 多维表格令牌
            table_id: 工作表ID，如果不指定则使用默认工作表
            filter_params: 查询过滤参数，格式为飞书API要求的过滤条件
            page_size: 每页记录数，默认100条
            
        Returns:
            多维表格记录列表
        """
        access_token = self.get_access_token()
        
        if not table_id:
            # 如果没有指定table_id，先获取默认table_id
            url = f"{self.base_url}/bitable/v1/spreadsheets/{app_token}"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; charset=utf-8"
            }
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            result = response.json()
            
            if result.get("code") != 0:
                raise Exception(f"获取多维表格信息失败: {result.get('msg')}")
            
            table_id = result.get("data", {}).get("spreadsheet", {}).get("sheets", [])[0].get("sheet_id")
        
        # 构建搜索请求参数
        search_params = {
            "page_size": page_size,
            "page_token": ""
        }
        
        if filter_params:
            search_params["filter"] = filter_params
        
        all_records = []
        
        # 使用search接口获取记录（支持分页）
        while True:
            url = f"{self.base_url}/bitable/v1/apps/{app_token}/tables/{table_id}/records/search"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; charset=utf-8"
            }
            
            response = requests.post(url, headers=headers, data=json.dumps(search_params))
            response.raise_for_status()
            result = response.json()
            
            if result.get("code") != 0:
                raise Exception(f"获取多维表格记录失败: {result.get('msg')}")
            
            # 添加当前页记录
            current_records = result.get("data", {}).get("items", [])
            all_records.extend(current_records)
            
            # 检查是否有下一页
            page_token = result.get("data", {}).get("page_token")
            if not page_token:
                break
            
            # 更新page_token继续获取下一页
            search_params["page_token"] = page_token
        
        return all_records
    
    def get_spreadsheet_sheets(self, spreadsheet_token: str) -> List[Dict[str, Any]]:
        """
        获取多维表格的所有工作表
        
        Args:
            spreadsheet_token: 多维表格令牌
            
        Returns:
            工作表列表
        """
        access_token = self.get_access_token()
        
        url = f"{self.base_url}/bitable/v1/spreadsheets/{spreadsheet_token}"
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        result = response.json()
        
        if result.get("code") != 0:
            raise Exception(f"获取多维表格信息失败: {result.get('msg')}")
        
        return result.get("data", {}).get("spreadsheet", {}).get("sheets", [])
    
    def batch_add_record(self, app_token:str, table_id: str, records: List[Dict[str, Any]]):
        access_token = self.get_access_token()
        
        url = f"{self.base_url}/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        payload = {
            "records": records
        }
        
        # 调试：打印完整请求体
        print(f"[DEBUG] 请求体: {json.dumps(payload, ensure_ascii=False)}")
        
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        result = response.json()
        
        if response.status_code != 200 or result.get("code") != 0:
            raise Exception(f"插入记录失败: {result.get('msg')}, 状态码: {response.status_code}, 完整响应: {json.dumps(result, ensure_ascii=False)}")
        
        return result.get("data", {})
    def add_spreadsheet_record(self, app_token: str, table_id: str, fields: Dict[str, Any]) -> Dict[str, Any]:
        """
        向多维表格中插入一条记录
        
        Args:
            app_token: 多维表格令牌
            table_id: 工作表ID
            fields: 要插入的字段数据，格式为飞书API要求的字段格式
            
        Returns:
            插入结果
        """
        access_token = self.get_access_token()
        
        url = f"{self.base_url}/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        payload = {
            "fields": fields
        }
        
        # 调试：打印完整请求体
        print(f"[DEBUG] 请求体: {json.dumps(payload, ensure_ascii=False)}")
        
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        result = response.json()
        
        if response.status_code != 200 or result.get("code") != 0:
            raise Exception(f"插入记录失败: {result.get('msg')}, 状态码: {response.status_code}, 完整响应: {json.dumps(result, ensure_ascii=False)}")
        
        return result.get("data", {})
