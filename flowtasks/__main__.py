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

from .utils import exe_func

# 启动项目
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    config_path = Path(sys.argv[-1] if len(sys.argv) > 1 else './config.json').resolve()
    config = json.load(open(config_path, 'r'))
    tasks = config['tasks']
    project_name = config['id']
    thread_pool = {}
    task_next = {}
    task_dep = {}
    for task in tasks:
        if task.get('skip'):
            continue

        task_name = task['id']
        thread_pool[task_name] = ThreadPoolExecutor(
            max_workers=task.get('max_workers', config.get('max_workers', os.cpu_count())))
        task_dep[task_name] = [next(filter(lambda x: x['id'] == n, tasks)) for n in task.get('deps', [])]
        for dep in task.get('deps', []):
            if dep not in task_next:
                task_next[dep] = [task]
            elif task not in task_next[dep]:
                task_next[dep].append(task)

    # 创建到数据库的连接引擎
    _engine = create_engine(config['db'],
                            pool_size=100,  # 连接池的大小
                            max_overflow=200,  # 超出固定大小后，允许创建的临时连接最大数量
                            pool_timeout=30,  # 获取连接的最大等待时间（秒）
                            pool_recycle=3600,  # 重新获取连接的频率，防止长时间没用的连接被数据库关闭
                            )
    Session = scoped_session(sessionmaker(bind=_engine))


    def init_db():
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
                db = Session()
                for t in diff_columns:
                    table.append_column(Column(t, JSON))
                    if t.endswith('_err'):
                        db.execute(text(f"alter table {project_name} add {t} varchar"))
                    else:
                        db.execute(text(f"alter table {project_name} add {t} json"))
                db.commit()
                db.close()
        except NoSuchTableError as e:
            table = Table(project_name, metadata)
            table.append_column(Column('id', String, primary_key=True, comment='业务唯一ID'))
            for t in tasks:
                table.append_column(Column(t['id'], JSON))
                table.append_column(Column(t['id'] + '_err', String))
            metadata.create_all(_engine)


    init_db()


    def check_task_can_run(task, row_dict):
        if task.get('skip'):
            return False
        task_name = task['id']
        # 如果已经执行过就跳过
        if row_dict.get(task_name) is not None or row_dict.get(task_name + '_err') is not None:
            return False

        # 检查依赖字段是否都存在
        deps = task_dep.get(task_name, [])
        for dep in deps:
            if row_dict.get(dep['id']) is None:
                return False  # 有依赖还没执行完，先退出下一个大循环再说

        return True


    def run_next_task(row_dict, task):
        task_name = task['id']
        next_tasks = task_next.get(task_name, [])
        if len(next_tasks) == 0:
            # 执行task_name之后的下一个为空的
            for t in tasks[tasks.index(task) + 1:]:
                if check_task_can_run(t, row_dict):
                    return thread_pool.get(t['id']).submit(run_task, t, row_dict)
        else:
            for next_task in next_tasks:
                if check_task_can_run(next_task, row_dict):
                    thread_pool.get(next_task['id']).submit(run_task, next_task, row_dict)


    # 运行一条工作流任务
    def run_task(task, row_dict):
        row_id = row_dict.get('id')
        task_name = task['id']
        logging.info('run_task:%s:%s', task_name, row_id)
        with ProcessPoolExecutor(max_workers=1) as process_executor:
            feature = process_executor.submit(exe_func, task, row_dict, config_path)
            # 这步会等待异步任务执行完成
            result, err = feature.result(timeout=task.get('timeout'))
        row_id = row_dict.get('id')
        db = Session()
        if err is not None:
            sql = text(f'update {project_name} set {task_name + "_err"} = :err where id = :row_id')
            db.execute(sql, {
                'err': err,
                'row_id': row_id
            })
            logging.error('task_err:%s:%s:%s', task_name, row_id, err)
        else:
            if result is None:
                result = {}
            db.execute(text(f"update {project_name} set {task_name} = :data where id = '{row_id}'"), {
                'data': json.dumps(result)
            })
            row_dict[task_name] = result
            logging.info('task_done:%s:%s', task_name, row_id)
        db.commit()
        db.close()
        run_next_task(row_dict, task)


    # 启动一个工作流任务
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
        db = Session()
        length = db.execute(text(f'''select count(*) 
        from {project_name} 
        where {' and '.join(where)}''')).fetchall()[0][0]
        logging.info('task_stat:%s:%s', task['id'], length)
        if length == 0:
            return
        # 查询大表通过游标避免内存占用
        rows = db.execute(text(
            f'''select {','.join(fields)} 
            from {project_name} 
            where {' and '.join(where)} 
            order by {task.get('order', config.get('order', 'random()'))}'''),
            execution_options={"stream_results": True})
        for row in rows:
            row_dict = dict(zip(fields, row))
            thread_pool.get(task['id']).submit(run_task, task, row_dict)
        db.close()


    # 运行一个种子任务
    def run_seed(seed):
        seed_name = seed['id']
        logging.info('run_seed:%s', seed_name)
        ids, err = exe_func(seed, {}, config_path)
        if err is not None:
            logging.error('seed_err:%s:%s', seed_name, err)
        elif ids is not None:
            sql = text(
                f'insert into {project_name} (id) values {','.join([f"('{id}')" for id in ids])} on conflict do nothing returning id')
            db = Session()
            rows = db.execute(sql)
            db.commit()
            db.close()
            logging.info('seed_done:%s:%s', seed_name, len(ids))
            for row in rows:
                row_dict = dict(zip(['id'], row))
                run_next_task(row_dict, tasks[0])


    # 启动所有种子任务
    def start_seeds():
        seeds = config.get('seeds', [])
        if len(seeds) == 0:
            return
        with ThreadPoolExecutor(max_workers=len(seeds)) as executor:
            executor.map(run_seed, seeds)


    # 启动所有任务
    with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
        executor.submit(start_seeds)
        for task in tasks:
            executor.submit(start_task, task)
        executor.shutdown(wait=True)
