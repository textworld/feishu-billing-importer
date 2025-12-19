#!/usr/bin/env python3
"""
飞书账单导入工具
用于导入和管理飞书账单数据
"""

import argparse
import sys
import os
import json
import datetime
import zipfile
import tempfile
import shutil
import re
import codecs
from typing import Dict, Any
from dotenv import load_dotenv

# 将当前目录添加到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.feishu_doc import FeishuDocClient
from app.core.config import settings


def convert_fields(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    转换记录字段格式为飞书API要求的格式
    
    Args:
        record: 原始记录字典
        
    Returns:
        转换后的记录字典
    """
    # 复制原始记录，避免修改原始数据
    converted_record = {}

    fields_map =   {
        "uniqId": "唯一字段",
        "date": "日期",
        "money": "金额",
        "name": "备注",
        "type": "收支",
        "batchNumber": "导入批次号"
    }

    for key, value in fields_map.items():
        if key in record:
            converted_record[value] = record[key]

    
    
    # 转换日期格式
    if '日期' in converted_record:
        try:
            # 尝试解析原始日期格式
            dt = datetime.datetime.strptime(converted_record['日期'], '%Y-%m-%d %H:%M:%S')
            # 转换为飞书API要求的时间戳格式（毫秒级）
            converted_record['日期'] = int(dt.timestamp() * 1000)
        except ValueError:
            # 如果解析失败，保持原始值
            pass
    
    # 将金额转换成浮点数
    if '金额' in converted_record:
        try:
            converted_record['金额'] = float(converted_record['金额'])
        except ValueError:
            # 如果解析失败，保持原始值
            pass
    
    return {
        "fields": converted_record
    }
def import_command(file_path: str, dry_run: bool = False, verbose: bool = False):
    """
    导入账单命令
    
    Args:
        file_path: 账单文件路径
        dry_run: 是否只模拟导入而不实际执行
        verbose: 是否显示详细日志
    """
    print(f"正在导入账单文件: {file_path}")
    
    # 生成批次号：格式为YYMMDD_HHMMSS
    now = datetime.datetime.now()
    batch_number = now.strftime("%y%m%d_%H%M%S")
    print(f"生成的批次号: {batch_number}")
    
    # 数据项列表
    data_item_list = []
    
    # 临时目录
    temp_dir = None
    
    try:
        # 检查文件是否存在
        if not os.path.exists(file_path):
            print(f"错误: 文件不存在 - {file_path}")
            return
        
        # 创建临时目录
        temp_dir = tempfile.mkdtemp()
        if verbose:
            print(f"[VERBOSE] 创建临时目录: {temp_dir}")
        
        # 解压缩zip文件
        if file_path.endswith('.zip'):
            if verbose:
                print(f"[VERBOSE] 正在解压zip文件: {file_path}")
            
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            if verbose:
                print(f"[VERBOSE] 解压完成")
        else:
            # 如果不是zip文件，直接处理
            temp_file_path = os.path.join(temp_dir, os.path.basename(file_path))
            shutil.copy2(file_path, temp_file_path)
        
        # 获取解压后的所有文件
        all_files = []
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                all_files.append(os.path.join(root, file))
        
        if verbose:
            print(f"[VERBOSE] 解压后的文件列表: {all_files}")
        
        # 筛选出CSV文件
        csv_files = [file for file in all_files if file.lower().endswith('.csv')]
        if verbose:
            print(f"[VERBOSE] CSV文件列表: {csv_files}")
        
        if not csv_files:
            print("错误: 未找到CSV文件")
            return
        
        # 遍历CSV文件
        for csv_file in csv_files:
            if verbose:
                print(f"[VERBOSE] 正在处理CSV文件: {csv_file}")
            
            # 读取文件内容（使用GB2312编码）
            try:
                with codecs.open(csv_file, 'r', encoding='gbk') as f:
                    file_content = f.read()
            except Exception as e:
                if verbose:
                    print(f"[VERBOSE] 尝试gbk编码失败，使用UTF-8编码: {str(e)}")
                try:
                    with open(csv_file, 'r', encoding='utf-8', errors='ignore') as f:
                        file_content = f.read()
                except Exception as e2:
                    print(f"错误: 读取文件失败 - {str(e2)}")
                    return

            # 按行分割文件内容
            lines = file_content.split('\n')
            
            # 遍历每一行
            for line in lines:
                line_trimmed = line.strip()
                if not line_trimmed:
                    continue
                
                # 检查是否以8位数字开头
                if re.match(r'^\d{8}', line_trimmed):
                    # 用逗号分隔行
                    columns = line_trimmed.split(',')
                    
                    if len(columns) >= 11:  # 确保有足够的列
                        uniq_id = columns[0].strip()
                        date = columns[3].strip()
                        money = columns[9].strip()
                        name = columns[8].strip()
                        type_ = columns[10].strip()
                        
                        if verbose:
                            print(f"[VERBOSE] 找到有效记录: ID={uniq_id}, Date={date}, Money={money}, Name={name}, Type={type_}")
                        
                        # 筛选收入或支出类型
                        if type_ in ['收入', '支出']:
                            data_item = {
                                'uniqId': uniq_id,
                                'date': date,
                                'money': money,
                                'name': name,
                                'type': type_,
                                'source': 'alipay',
                                'batchNumber': batch_number
                            }
                            data_item_list.append(data_item)
        
        # 输出处理完成的数据
        print(f"\n处理完成，共提取到 {len(data_item_list)} 条记录")
        print(json.dumps({'data': data_item_list}, ensure_ascii=False, indent=2))

        # 将 data_item_list 中的每个元素通过 convert_fields 转换成新对象
        converted_data_list = [convert_fields(item) for item in data_item_list]

        # 输出处理完成的数据
        print(f"\n处理完成，共提取到 {len(converted_data_list)} 条记录")
        print(json.dumps({'data': converted_data_list}, ensure_ascii=False, indent=2))
        
        if dry_run:
            print("\n[DRY RUN] 模拟导入，不会实际执行飞书插入操作")
            return
        
        if verbose:
            print("[VERBOSE] 显示详细日志信息")
        
        # 检查飞书配置是否存在
        if not all([settings.FEISHU_APP_ID, settings.FEISHU_APP_SECRET, settings.FEISHU_APP_TOKEN]):
            print("错误: 飞书配置不完整，请检查配置文件")
            return
        
        # 创建飞书文档客户端
        client = FeishuDocClient(
            app_id=settings.FEISHU_APP_ID,
            app_secret=settings.FEISHU_APP_SECRET
        )
        
        # 向飞书多维表格插入一条记录
        # 飞书API字段格式：直接使用字符串值即可
        fields = {
            "导入批次编号": batch_number,
            "时间": int(now.timestamp() * 1000)  # 飞书API使用毫秒级时间戳
        }
        
        if verbose:
            print(f"[VERBOSE] 准备插入的字段: {json.dumps(fields, ensure_ascii=False, indent=2)}")
        
        # 插入记录
        result = client.add_spreadsheet_record(
            app_token=settings.FEISHU_APP_TOKEN,
            table_id=settings.FEISHU_TABLE_ID_BATCH_NUMBER,
            fields=fields
        )
        
        print("\n成功向飞书多维表格插入记录")
        if verbose:
            print(f"[VERBOSE] 插入结果: {json.dumps(result, ensure_ascii=False, indent=2)}")

        client.batch_add_record(
            app_token=settings.FEISHU_APP_TOKEN,
            table_id=settings.FEISHU_TABLE_ID_BILLING,
            records=converted_data_list
        )
            
    except Exception as e:
        print(f"错误: 导入过程中发生错误 - {str(e)}")
        if verbose:
            import traceback
            traceback.print_exc()
    finally:
        # 清理临时目录
        if temp_dir and os.path.exists(temp_dir):
            if verbose:
                print(f"[VERBOSE] 清理临时目录: {temp_dir}")
            shutil.rmtree(temp_dir, ignore_errors=True)


def list_command(filter_type: str = None, status: str = None):
    """
    列出账单命令
    
    Args:
        filter_type: 过滤账单类型
        status: 过滤账单状态
    """
    print("正在列出账单")
    if filter_type:
        print(f"过滤条件: 类型 = {filter_type}")
    if status:
        print(f"过滤条件: 状态 = {status}")
    
    # 调用飞书文档接口获取多维表格内容
    try:
        # 检查飞书配置是否存在
        if not all([settings.FEISHU_APP_ID, settings.FEISHU_APP_SECRET, settings.FEISHU_APP_TOKEN]):
            print("错误: 飞书配置不完整，请检查配置文件")
            return
        
        # 创建飞书文档客户端
        client = FeishuDocClient(
            app_id=settings.FEISHU_APP_ID,
            app_secret=settings.FEISHU_APP_SECRET
        )
        
        # 获取多维表格记录
        records = client.get_spreadsheet_records(
            app_token=settings.FEISHU_APP_TOKEN,
            table_id=settings.FEISHU_TABLE_ID_BATCH_NUMBER
        )
        
        # 打印记录信息
        print(f"\n获取到 {len(records)} 条记录:")
        # 以JSON格式打印所有记录
        print(json.dumps(records, ensure_ascii=False, indent=2, default=str))
                
    except Exception as e:
        print(f"错误: 获取多维表格内容失败 - {str(e)}")


def main():
    """
    主函数，解析命令行参数并执行相应命令
    """
    # 加载.env文件
    load_dotenv()
    
    # 创建解析器
    parser = argparse.ArgumentParser(description="飞书账单导入工具")
    
    # 创建子命令解析器
    subparsers = parser.add_subparsers(dest="command", help="可用命令")
    
    # 导入命令
    import_parser = subparsers.add_parser("import", help="导入账单")
    import_parser.add_argument("file_path", type=str, help="账单文件路径")
    import_parser.add_argument("--dry-run", action="store_true", help="只模拟导入而不实际执行")
    import_parser.add_argument("-v", "--verbose", action="store_true", help="显示详细日志")
    
    # 列出命令
    list_parser = subparsers.add_parser("ls", help="列出账单")
    list_parser.add_argument("--type", type=str, help="过滤账单类型")
    list_parser.add_argument("--status", type=str, help="过滤账单状态")
    
    # 解析参数
    args = parser.parse_args()
    
    # 执行相应命令
    if args.command == "import":
        import_command(args.file_path, args.dry_run, args.verbose)
    elif args.command == "ls":
        list_command(args.type, args.status)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
