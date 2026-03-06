"""数据清洗工具 — Web版"""
import streamlit as st
import tempfile
import os
import sys
import io
import zipfile
import threading
import importlib

# === 路径设置 ===
APP_DIR = os.path.dirname(os.path.abspath(__file__))
CLEAN_DIR = os.path.join(APP_DIR, 'clean')

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
        'output_files': ['清洗_公域订单.xlsx'],
        'help': '上传ZIP文件，内含 YYYYMM店铺名/ 目录结构（如 202501抖音旗舰店/）',
    },
    {
        'name': 'CRM清洗',
        'module_name': 'clean_crm_data',
        'output_files': ['清洗_私域订单.xlsx'],
        'help': '上传ZIP文件，内含CRM订单和退款Excel文件',
    },
    {
        'name': '微信账单清洗',
        'module_name': 'clean_wechat_bill',
        'output_files': ['微信账单清洗.xlsx'],
        'help': '上传ZIP文件，内含 现金/ 子目录和CSV账单文件',
    },
    {
        'name': '支付宝/余利宝账单清洗',
        'module_name': 'clean_alipay_bill',
        'output_files': ['支付宝账单清洗.xlsx'],
        'help': '上传支付宝账单ZIP文件，可同时上传余利宝XLSX文件',
    },
    {
        'name': '银行账单清洗',
        'module_name': 'clean_bank_bill',
        'output_files': ['银行账单清洗.xlsx'],
        'help': '上传银行账单XLSX文件 + Mst_银行账单映射.xlsx（映射表）',
    },
]

# 全局锁：防止并发修改模块变量
_module_lock = threading.Lock()


def _extract_uploads(uploaded_files, input_dir):
    """保存上传文件到临时目录，自动解压ZIP"""
    for f in uploaded_files:
        filepath = os.path.join(input_dir, f.name)
        with open(filepath, 'wb') as out:
            out.write(f.getbuffer())
        if f.name.lower().endswith('.zip'):
            try:
                with zipfile.ZipFile(filepath, 'r') as zf:
                    zf.extractall(input_dir)
            except zipfile.BadZipFile:
                st.warning(f"无法解压: {f.name}，已跳过")


def _run_module(module_name, input_dir, output_dir):
    """导入并执行清洗模块，返回日志文本"""
    mod = importlib.import_module(module_name)

    # 保存原始值
    orig_output = getattr(mod, 'OUTPUT_DIR', None)

    # 猴子补丁：重定向输出目录
    mod.OUTPUT_DIR = output_dir

    # 屏蔽 save_cache（web版不需要缓存路径）
    import utils
    orig_save_cache = utils.save_cache
    utils.save_cache = lambda *a, **kw: None

    # 屏蔽 input()（防止挂起）
    import builtins
    orig_input = builtins.input
    builtins.input = lambda *a, **kw: ''

    # 捕获 print 输出
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


def main():
    st.set_page_config(page_title="数据清洗工具", page_icon="🧹", layout="centered")
    st.title("数据清洗工具")
    st.caption("上传数据文件，选择清洗类型，下载处理结果")

    # 模块选择
    module_names = [m['name'] for m in MODULES]
    selected_idx = st.selectbox("选择清洗类型", range(len(module_names)),
                                format_func=lambda i: module_names[i])
    module_info = MODULES[selected_idx]
    st.info(module_info['help'])

    # 文件上传
    uploaded_files = st.file_uploader(
        "上传数据文件（支持ZIP/XLSX/XLS/CSV）",
        accept_multiple_files=True,
        type=['zip', 'xlsx', 'xls', 'csv']
    )

    # 处理按钮
    if st.button("开始清洗", type="primary", disabled=not uploaded_files):
        if not uploaded_files:
            st.warning("请先上传文件")
            return

        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = os.path.join(tmpdir, 'input')
            output_dir = os.path.join(tmpdir, 'output')
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(output_dir, exist_ok=True)

            # 保存并解压上传文件
            _extract_uploads(uploaded_files, input_dir)

            # 执行清洗
            with _module_lock:
                with st.spinner("正在处理，请稍候..."):
                    try:
                        log_text = _run_module(module_info['module_name'], input_dir, output_dir)
                    except Exception as e:
                        st.error(f"处理出错: {e}")
                        import traceback
                        st.code(traceback.format_exc())
                        return

            st.success("处理完成!")

            # 显示日志
            if log_text:
                with st.expander("处理日志", expanded=False):
                    st.text(log_text)

            # 下载按钮
            found_output = False
            for fname in os.listdir(output_dir):
                filepath = os.path.join(output_dir, fname)
                if os.path.isfile(filepath) and fname.endswith('.xlsx'):
                    with open(filepath, 'rb') as f:
                        st.download_button(
                            f"下载 {fname}",
                            f.read(),
                            file_name=fname,
                            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                        )
                    found_output = True

            if not found_output:
                st.warning("未生成输出文件，请检查上传的数据是否正确")


if __name__ == '__main__':
    main()
