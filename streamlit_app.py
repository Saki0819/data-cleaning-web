"""数据清洗工具 — Web版"""
import streamlit as st
import tempfile
import os
import sys
import io
import json
import zipfile
import shutil
import threading
import importlib
from datetime import datetime

# === 路径设置 ===
APP_DIR = os.path.dirname(os.path.abspath(__file__))
CLEAN_DIR = os.path.join(APP_DIR, 'clean')
# exe部署目录（本机持久化配置）
CLEAN_EDIT_DIR = os.path.normpath(os.path.join(APP_DIR, '..', 'tool', 'tool', 'cleanEdit'))

sys.path.insert(0, CLEAN_DIR)
for sub in ['clean_shop', 'clean_crm', 'clean_wechat', 'clean_alipay', 'clean_bank']:
    p = os.path.join(CLEAN_DIR, sub)
    if p not in sys.path:
        sys.path.insert(0, p)

# === 模块定义 ===
MODULES = [
    {
        'name': '公域订单清洗',
        'module_name': 'clean_shop_data',
        'help': '上传ZIP文件，内含 YYYYMM店铺名/ 目录结构（如 202501抖音旗舰店/）',
    },
    {
        'name': 'CRM清洗',
        'module_name': 'clean_crm_data',
        'help': '上传ZIP文件，内含CRM订单和退款Excel文件',
    },
    {
        'name': '微信账单清洗',
        'module_name': 'clean_wechat_bill',
        'help': '上传ZIP文件，内含 现金/ 子目录和CSV账单文件',
    },
    {
        'name': '支付宝/余利宝账单清洗',
        'module_name': 'clean_alipay_bill',
        'help': '上传支付宝账单ZIP文件，可同时上传余利宝XLSX文件',
    },
    {
        'name': '银行账单清洗',
        'module_name': 'clean_bank_bill',
        'help': '上传银行账单XLSX文件（映射表自动加载，无需上传）',
    },
    {
        'name': '管理直播时间规则',
        'module_name': None,
        'help': '查看、添加、删除真直播时间区间规则',
    },
]

# 银行映射表持久路径
BANK_MST_PATH = os.path.join(CLEAN_EDIT_DIR, 'clean_bank', 'Mst_银行账单映射.xlsx')
# CRM配置持久路径
CRM_CONFIG_PATH = os.path.join(CLEAN_EDIT_DIR, 'clean_crm', 'crm_config.json')

# 全局锁
_module_lock = threading.Lock()


def _extract_uploads(uploaded_files, input_dir):
    """保存上传文件到临时目录，自动解压ZIP（处理GBK编码的中文文件名）"""
    for f in uploaded_files:
        filepath = os.path.join(input_dir, f.name)
        with open(filepath, 'wb') as out:
            out.write(f.getbuffer())
        if f.name.lower().endswith('.zip'):
            try:
                with zipfile.ZipFile(filepath, 'r') as zf:
                    for info in zf.infolist():
                        # 修复GBK编码的中文文件名
                        try:
                            fixed_name = info.filename.encode('cp437').decode('gbk')
                        except (UnicodeDecodeError, UnicodeEncodeError):
                            fixed_name = info.filename
                        # 跳过目录项中的 __MACOSX 等垃圾
                        if fixed_name.startswith('__'):
                            continue
                        target = os.path.join(input_dir, fixed_name)
                        if info.is_dir():
                            os.makedirs(target, exist_ok=True)
                        else:
                            os.makedirs(os.path.dirname(target), exist_ok=True)
                            with zf.open(info) as src, open(target, 'wb') as dst:
                                dst.write(src.read())
            except zipfile.BadZipFile:
                st.warning(f"无法解压: {f.name}，已跳过")


def _find_data_dir(input_dir):
    """ZIP解压后自动定位真实数据目录。
    递归向下：如果只有一个子文件夹且无数据文件，继续深入（最多3层）。"""
    current = input_dir
    for _ in range(3):
        entries = os.listdir(current)
        # 排除 __MACOSX、.DS_Store 等垃圾
        dirs = [e for e in entries
                if os.path.isdir(os.path.join(current, e)) and not e.startswith(('__', '.'))]
        files = [e for e in entries
                 if os.path.isfile(os.path.join(current, e))
                 and not e.lower().endswith('.zip') and not e.startswith(('__', '.'))]
        if len(dirs) == 1 and not files:
            current = os.path.join(current, dirs[0])
        else:
            break
    return current


def _inject_bank_mst(data_dir):
    """银行清洗前：自动将持久化的Mst映射表复制到数据目录。
    如果数据目录中有用户上传的新Mst，则保存到持久路径供下次使用。"""
    mst_name = os.path.basename(BANK_MST_PATH)
    uploaded_mst = os.path.join(data_dir, mst_name)

    if os.path.exists(uploaded_mst):
        # 用户本次上传了新Mst → 保存到持久路径
        os.makedirs(os.path.dirname(BANK_MST_PATH), exist_ok=True)
        shutil.copy2(uploaded_mst, BANK_MST_PATH)
        return True
    elif os.path.exists(BANK_MST_PATH):
        # 使用之前保存的Mst
        shutil.copy2(BANK_MST_PATH, uploaded_mst)
        return True
    return False


def _run_module(module_name, input_dir, output_dir):
    """导入并执行清洗模块，返回日志文本"""
    mod = importlib.import_module(module_name)

    orig_output = getattr(mod, 'OUTPUT_DIR', None)
    mod.OUTPUT_DIR = output_dir

    import utils
    orig_save_cache = utils.save_cache
    utils.save_cache = lambda *a, **kw: None

    import builtins
    orig_input = builtins.input
    builtins.input = lambda *a, **kw: ''

    log = io.StringIO()
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    sys.stdout = log
    sys.stderr = log

    try:
        mod.main(input_paths=[input_dir])
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        builtins.input = orig_input
        utils.save_cache = orig_save_cache
        if orig_output is not None:
            mod.OUTPUT_DIR = orig_output

    return log.getvalue()


# === 直播时间规则管理 ===

def _get_crm_config_path():
    """获取CRM配置路径：优先使用cleanEdit持久化版本"""
    if os.path.exists(CRM_CONFIG_PATH):
        return CRM_CONFIG_PATH
    return os.path.join(CLEAN_DIR, 'clean_crm', 'crm_config.json')


def _load_live_rules():
    path = _get_crm_config_path()
    with open(path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    return config.get('直播形式', {}).get('真直播时间段', [])


def _save_live_rules(rules):
    path = _get_crm_config_path()
    with open(path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    config.setdefault('直播形式', {})['真直播时间段'] = rules
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


def _render_live_rules():
    """直播时间规则管理页面"""
    st.subheader("真直播时间规则")

    rules = _load_live_rules()

    # 显示现有规则
    if rules:
        st.write(f"当前共 **{len(rules)}** 条规则：")
        for i, r in enumerate(rules):
            col1, col2 = st.columns([4, 1])
            col1.write(f"`{r['开始']}` ~ `{r['结束']}`")
            if col2.button("删除", key=f"del_{i}"):
                removed = rules.pop(i)
                _save_live_rules(rules)
                st.success(f"已删除: {removed['开始']} ~ {removed['结束']}")
                st.rerun()
    else:
        st.info("暂无时间规则")

    # 添加新规则
    st.divider()
    st.write("**添加新规则**")
    col_start, col_end = st.columns(2)
    start_date = col_start.date_input("开始日期", value=None, key="start_date")
    end_date = col_end.date_input("结束日期", value=None, key="end_date")
    col_start_time, col_end_time = st.columns(2)
    start_time = col_start_time.time_input("开始时间", value=None, key="start_time")
    end_time = col_end_time.time_input("结束时间", value=None, key="end_time")

    if st.button("添加", type="primary"):
        if not all([start_date, end_date, start_time, end_time]):
            st.warning("请填写完整的日期和时间")
            return
        start_str = f"{start_date.strftime('%Y-%m-%d')} {start_time.strftime('%H:%M')}"
        end_str = f"{end_date.strftime('%Y-%m-%d')} {end_time.strftime('%H:%M')}"
        rules.append({'开始': start_str, '结束': end_str})
        _save_live_rules(rules)
        st.success(f"已添加: {start_str} ~ {end_str}")
        st.rerun()


# === 主界面 ===

def main():
    st.set_page_config(page_title="数据清洗工具", page_icon="🧹", layout="centered")
    st.title("数据清洗工具")
    st.caption("上传数据文件，选择清洗类型，下载处理结果")

    module_names = [m['name'] for m in MODULES]
    selected_idx = st.selectbox("选择清洗类型", range(len(module_names)),
                                format_func=lambda i: module_names[i])
    module_info = MODULES[selected_idx]

    # 直播时间规则：独立页面，不需要上传文件
    if module_info['module_name'] is None:
        _render_live_rules()
        return

    st.info(module_info['help'])

    uploaded_files = st.file_uploader(
        "上传数据文件（支持ZIP/XLSX/XLS/CSV）",
        accept_multiple_files=True,
        type=['zip', 'xlsx', 'xls', 'csv']
    )

    if st.button("开始清洗", type="primary", disabled=not uploaded_files):
        if not uploaded_files:
            st.warning("请先上传文件")
            return

        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = os.path.join(tmpdir, 'input')
            output_dir = os.path.join(tmpdir, 'output')
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(output_dir, exist_ok=True)

            _extract_uploads(uploaded_files, input_dir)
            data_dir = _find_data_dir(input_dir)
            # 调试：显示定位到的目录和内容
            st.caption(f"数据目录: .../{os.path.basename(data_dir)}/")
            try:
                contents = os.listdir(data_dir)
                dirs_in = [c for c in contents if os.path.isdir(os.path.join(data_dir, c))]
                files_in = [c for c in contents if os.path.isfile(os.path.join(data_dir, c))]
                if dirs_in:
                    st.caption(f"子目录: {', '.join(dirs_in[:10])}")
                if files_in:
                    st.caption(f"文件: {', '.join(files_in[:10])}")
            except Exception:
                pass

            # 银行清洗：自动注入Mst映射表
            if module_info['module_name'] == 'clean_bank_bill':
                if not _inject_bank_mst(data_dir):
                    st.error("首次使用请同时上传 Mst_银行账单映射.xlsx，之后会自动记住")
                    return

            with _module_lock:
                with st.spinner("正在处理，请稍候..."):
                    try:
                        log_text = _run_module(module_info['module_name'], data_dir, output_dir)
                    except Exception as e:
                        st.error(f"处理出错: {e}")
                        import traceback
                        st.code(traceback.format_exc())
                        return

            # 将输出文件缓存到 session_state（防止下载时页面刷新丢失）
            output_files = {}
            for fname in os.listdir(output_dir):
                filepath = os.path.join(output_dir, fname)
                if os.path.isfile(filepath) and fname.endswith('.xlsx'):
                    with open(filepath, 'rb') as f:
                        output_files[fname] = f.read()

            st.session_state['output_files'] = output_files
            st.session_state['log_text'] = log_text

    # 显示缓存的结果（不在 button 块内，刷新后仍可见）
    if 'output_files' in st.session_state and st.session_state['output_files']:
        st.success("处理完成!")

        log_text = st.session_state.get('log_text', '')
        if log_text:
            with st.expander("处理日志", expanded=False):
                st.text(log_text)

        for fname, data in st.session_state['output_files'].items():
            st.download_button(
                f"下载 {fname}",
                data,
                file_name=fname,
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                key=f"dl_{fname}"
            )
    elif 'output_files' in st.session_state:
        st.warning("未生成输出文件，请检查上传的数据是否正确")


if __name__ == '__main__':
    main()
