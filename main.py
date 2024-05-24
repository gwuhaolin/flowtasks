import importlib
import json
import logging
import os
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path

from sqlalchemy import JSON
from sqlalchemy import MetaData, Table, Column, String, create_engine
from sqlalchemy import text
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.orm import sessionmaker, scoped_session


def exe_task(task, row_dict):  # 给进程用的任务执行
    # 已知python文件的filepath和该文件里面一个函数的func_name，获取该func的函数引用，用于执行该func
    # 基于文件路径创建模块 spec
    spec = importlib.util.spec_from_file_location(task['id'], task['filepath'])
    # 生成模块对象
    module = importlib.util.module_from_spec(spec)
    # 装载模块
    spec.loader.exec_module(module)
    # 获取模块中的函数引用
    task_func = getattr(module, task['id'])
    try:
        return task_func(row_dict), None
    except Exception as e:
        return None, e.__str__()  #


# 启动项目
def run_project(config, loglevel=logging.INFO):
    logging.basicConfig(level=loglevel)
    log = logging.getLogger()
    tasks = config['tasks']
    project_name = config['id']

    def init_db():
        # 创建到数据库的连接引擎
        _engine = create_engine(config['db'])
        _db = scoped_session(sessionmaker(bind=_engine))

        metadata = MetaData()
        try:
            table = Table(project_name, metadata, autoload_with=_engine)
            real_columns = [c.name for c in table.columns]
            all_fields = ['id']
            for task in tasks:
                task_name = task['id']
                all_fields.append(task_name)
                all_fields.append(task_name + '_err')
            diff_columns = set(all_fields).difference(real_columns)
            if len(diff_columns) > 0:
                for t in diff_columns:
                    table.append_column(Column(t, JSON))
                    if t.endswith('_err'):
                        _db.execute(text(f"alter table {project_name} add {t} varchar"))
                    else:
                        _db.execute(text(f"alter table {project_name} add {t} json"))
                _db.commit()
        except NoSuchTableError as e:
            table = Table(project_name, metadata)
            table.append_column(Column('id', String, primary_key=True, comment='业务唯一ID'))
            for t in tasks:
                table.append_column(Column(t['id'], JSON))
                table.append_column(Column(t['id'] + '_err', String))
            metadata.create_all(_engine)
        finally:
            return _db

    db = init_db()

    # 任务完成
    def task_done(task_name, row_id, new_data):
        db.execute(text(f"update {project_name} set {task_name} = :data where id = '{row_id}'"), {
            'data': json.dumps(new_data)
        })
        db.commit()

    # 任务异常
    def task_err(task_name, row_id, err):
        sql = text(f'update {project_name} set {task_name + "_err"} = :err where id = :row_id')
        db.execute(sql, {
            'err': err,
            'row_id': row_id
        })
        db.commit()

    def run_task(task, row_dict, process_executor):
        row_id = row_dict.get('id')
        task_name = task['id']
        log.info('run_task:%s:%s', task_name, row_id)
        feature = process_executor.submit(exe_task, task, row_dict)
        result, err = feature.result(timeout=task.get('timeout'))
        if err is not None:
            task_err(task_name, row_id, err)
            log.error('task_err:%s:%s:%s', task_name, row_id, err)
        else:
            if result is None:
                result = {}
            task_done(task_name, row_id, result)
            row_dict[task_name] = result
            log.info('task_done:%s:%s', task_name, row_id)

    def start_task(task):
        if task.get('skip'):
            return
        # 启动项目下所有task对应的todos
        fields = ['id']
        where = [
            f'{task['id']} isnull',
            f'{task['id']}_err isnull',
        ]
        for dep in task.get('deps', []):
            fields.append(dep)
            where.append(f'{dep} notnull')
        length = db.execute(text(f'''select count(*) 
        from {project_name} 
        where {' and '.join(where)}''')).fetchall()[0][0]
        log.info('task_stat:%s:%s', task['id'], length)
        if length == 0:
            return
        # 查询大表通过游标避免内存占用
        rows = db.execute(text(
            f'''select {','.join(fields)} 
            from {project_name} 
            where {' and '.join(where)} 
            order by {task.get('order', config.get('order', 'random()'))}'''),
            execution_options={"stream_results": True})
        max_workers = task.get('max_workers', os.cpu_count())
        with ProcessPoolExecutor(max_workers=max_workers) as process_executor:
            with ThreadPoolExecutor(max_workers) as thread_executor:
                for row in rows:
                    row_dict = dict(zip(fields, row))
                    thread_executor.submit(run_task, task, row_dict, process_executor)
                thread_executor.shutdown(wait=True)
            process_executor.shutdown(wait=True)

    with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
        executor.map(start_task, tasks)


if __name__ == '__main__':
    config_path = Path(sys.argv[-1] if len(sys.argv) > 1 else './config.json').resolve()
    config = json.load(open(config_path, 'r'))
    run_project(config)
