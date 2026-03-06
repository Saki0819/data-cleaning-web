"""真直播时间区间管理模块"""
import os
import sys
import json
import re
from datetime import datetime

if getattr(sys, 'frozen', False):
    SCRIPT_DIR = os.path.join(os.path.dirname(sys.executable), 'clean_crm')
else:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def _config_path():
    return os.path.join(SCRIPT_DIR, 'crm_config.json')


def load_rules(config_path=None):
    """从 crm_config.json 读取真直播时间段列表"""
    path = config_path or _config_path()
    with open(path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    return config.get('直播形式', {}).get('真直播时间段', [])


def save_rules(rules, config_path=None):
    """写回 crm_config.json（仅更新真直播时间段字段）"""
    path = config_path or _config_path()
    with open(path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    config.setdefault('直播形式', {})['真直播时间段'] = rules
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


def display_rules(rules):
    """打印所有时间段"""
    if not rules:
        print('  (无时间规则)')
        return
    for i, r in enumerate(rules):
        print(f'  [{i + 1}] {r["开始"]} ~ {r["结束"]}')


def _parse_time_range(s):
    """解析输入的时间区间字符串，支持 / 和 - 分隔日期，返回 (开始str, 结束str) 或 None"""
    s = s.strip()
    # 匹配: 日期时间-日期时间（中间用 - 连接两个时间）
    # 支持 2026/02/06 18:00-2026/02/06 20:00 或 2026-02-06 18:00-2026-02-06 20:00
    m = re.match(
        r'(\d{4}[/-]\d{2}[/-]\d{2}\s+\d{2}:\d{2})\s*[-~]\s*(\d{4}[/-]\d{2}[/-]\d{2}\s+\d{2}:\d{2})',
        s
    )
    if not m:
        return None
    start_s, end_s = m.group(1).replace('/', '-'), m.group(2).replace('/', '-')
    # 验证可解析
    try:
        datetime.strptime(start_s, '%Y-%m-%d %H:%M')
        datetime.strptime(end_s, '%Y-%m-%d %H:%M')
    except ValueError:
        return None
    return start_s, end_s


def interactive_manage(config_path=None):
    """交互式管理真直播时间规则（查看/添加/删除）"""
    path = config_path or _config_path()
    rules = load_rules(path)

    while True:
        print('\n--- 真直播时间规则 ---')
        display_rules(rules)
        print('\n  [a] 添���  [d] 删除  [q] 返回')
        choice = input('  操作: ').strip().lower()

        if choice == 'a':
            print('  格式: 2026-02-06 18:00-2026-02-06 20:00')
            inp = input('  输入时间段: ').strip()
            parsed = _parse_time_range(inp)
            if parsed is None:
                print('  ✗ 格式错误')
                continue
            rules.append({'开始': parsed[0], '结束': parsed[1]})
            save_rules(rules, path)
            print(f'  ✓ 已添加: {parsed[0]} ~ {parsed[1]}')

        elif choice == 'd':
            if not rules:
                print('  无规则可删除')
                continue
            idx = input('  输入序号: ').strip()
            if not idx.isdigit() or int(idx) < 1 or int(idx) > len(rules):
                print('  ✗ 无效序号')
                continue
            removed = rules.pop(int(idx) - 1)
            save_rules(rules, path)
            print(f'  ✓ 已删除: {removed["开始"]} ~ {removed["结束"]}')

        elif choice == 'q':
            break
