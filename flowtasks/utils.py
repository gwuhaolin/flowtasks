import importlib
from pathlib import Path


def exe_func(func, func_params, config_path):  # 给进程用的任务执行
    # 已知python文件的filepath和该文件里面一个函数的func_name，获取该func的函数引用，用于执行该func
    # 基于文件路径创建模块 spec
    func_path = func['filepath']
    if not func_path.startswith('/'):
        func_path = (Path(config_path).parent / Path(func_path)).resolve()
    spec = importlib.util.spec_from_file_location(func['id'], func_path)
    # 生成模块对象
    module = importlib.util.module_from_spec(spec)
    # 装载模块
    spec.loader.exec_module(module)
    # 获取模块中的函数引用
    task_func = getattr(module, func['id'])
    try:
        return task_func(func_params), None
    except Exception as e:
        return None, e.__str__()  #
