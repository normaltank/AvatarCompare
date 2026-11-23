from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import sys
import time
import webbrowser

if '--max-resources' in sys.argv:
    for variable in ['OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'OMP_NUM_THREADS']:
        os.environ[variable] = '1'

from .engine import search
from .report import write_reports
from .range_engine import search_range, validate_range
from .source import QLOGO_ENDPOINT, QLOGO_SOURCE_NAMES, SourceConfig, cache_batch
from .cache_lock import CacheLease


def main() -> int:
    parser = argparse.ArgumentParser(description='比较本地图片或获授权测试接口的编号范围，输出图片同源证据。')
    parser.add_argument('--target', type=Path, help='目标图片')
    parser.add_argument('--folder', type=Path, help='候选图片文件夹，含子文件夹')
    parser.add_argument('--start', type=int, help='起始编号（包含）')
    parser.add_argument('--end', type=int, help='结束编号（包含）')
    parser.add_argument('--endpoint', help='图片接口 URL 模板；默认使用用户指定的 q1.qlogo.cn 100像素接口')
    parser.add_argument('--source-revision', help='显式指定缓存批次；不填则自动创建并复用批次')
    parser.add_argument('--refresh-source', action='store_true', help='创建新缓存批次并重新获取整个范围')
    parser.add_argument('--source-name', help='报告中的来源名称')
    parser.add_argument('--token-env', help='保存 Bearer 令牌的环境变量名')
    parser.add_argument('--rate', type=float, help='共享请求速率；普通模式默认2次/秒，全速默认0（不固定等待）')
    parser.add_argument('--full-speed', action='store_true', help='连接复用、下载/计算分离、磁盘记录；默认不固定等待，遇限速退避')
    parser.add_argument('--max-resources', action='store_true', help='启用全速，默认使用全部逻辑CPU计算及每CPU4路下载')
    parser.add_argument('--compute-backend', choices=['thread', 'process'], help='特征计算模式；全部资源默认process，其他默认thread')
    parser.add_argument('--pipeline', choices=['legacy','staged'], help='全速默认staged独立下载/计算/提交；legacy用于对照')
    parser.add_argument('--share-computations', action='store_true', help='合并相同头像的计算并复用热结果；重复较多时使用，默认关闭，仅staged')
    parser.add_argument('--compact', action='store_true', help='单目标省空间：仅留匹配候选/前100复核项和错误，压缩记录已处理区间；默认独立任务库')
    parser.add_argument('--buffer-mib', type=int, default=256, help='staged原始响应缓冲预算MiB（不含特征、SQLite及网络缓冲），默认256')
    parser.add_argument('--download-workers', type=int, choices=range(1, 257), help='下载并行数，全速默认每CPU4路（16到128），全部资源最多256')
    parser.add_argument('--timeout', type=float, default=10, help='网络读写超时秒数，默认10')
    parser.add_argument('--retries', type=int, default=2, help='失败后的额外重试次数，默认2')
    parser.add_argument('--retry-errors', action='store_true', help='同一任务只重试失败编号')
    parser.add_argument('--remember-source', action='store_true', help='在省空间模式下保存编号、指纹和去重原图，供同批次换目标复用')
    parser.add_argument('--state-dir', type=Path, help='任务库目录；默认.state，省空间模式默认.state-compact')
    parser.add_argument('--output', type=Path, default=Path(__file__).resolve().parents[1]/'outputs')
    parser.add_argument('--workers', type=int, choices=range(1, max(4, min(os.cpu_count() or 1, 256))+1), help='范围计算并行数，普通默认2，全速最多8，全部资源默认逻辑CPU数')
    parser.add_argument('--note', default='', help='写入报告的资料来源或实验说明')
    parser.add_argument('--limit', type=int, default=20, help='报告展示数量，1到100')
    parser.add_argument('--pick', action='store_true', help='使用系统窗口选择图片和文件夹')
    parser.add_argument('--pick-range', action='store_true', help='使用系统窗口选择目标并输入起止编号')
    parser.add_argument('--open-report', action='store_true')
    args = parser.parse_args()
    args.state_dir = args.state_dir or Path(__file__).resolve().parents[1]/('.state-compact' if args.compact else '.state')
    if args.max_resources:
        args.full_speed = True
    args.workers = args.workers or (min(os.cpu_count() or 1, 256) if args.max_resources
                                   else min(8,os.cpu_count() or 1) if args.full_speed else 2)
    if args.pick and args.pick_range:
        parser.error('--pick 和 --pick-range 不能同时使用')
    if args.pick or args.pick_range:
        import tkinter as tk
        from tkinter import filedialog, simpledialog
        root = tk.Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        target = filedialog.askopenfilename(title='选择目标图片', filetypes=[('图片', '*.png *.jpg *.jpeg *.webp *.bmp *.tif *.tiff *.gif')], parent=root)
        if not target:
            root.destroy()
            return 0
        folder = None
        if args.pick_range:
            for name, prompt in [('start', '起始编号（包含）'), ('end', '结束编号（包含）')]:
                initial = getattr(args, name)
                value = simpledialog.askstring('范围比对', prompt, initialvalue=str(initial) if initial is not None else '', parent=root)
                if not value:
                    root.destroy()
                    return 0
                if name in {'start', 'end'}:
                    try:
                        value = int(value)
                    except ValueError:
                        root.destroy()
                        parser.error('起止编号必须为整数')
                setattr(args, name, value)
        else:
            folder = filedialog.askdirectory(title='选择你有权使用的候选图片文件夹', parent=root)
        root.destroy()
        if not args.pick_range and not folder:
            return 0
        args.target, args.folder = Path(target), Path(folder) if folder else None
    range_mode = any(value is not None for value in (args.start, args.end, args.endpoint, args.source_revision))
    if not args.target or (not range_mode and not args.folder):
        parser.error('请提供 --target 和 --folder，或 --target、--start、--end。')
    if range_mode and (args.folder or args.start is None or args.end is None):
        parser.error('范围模式须完整提供起止编号，且不混用 --folder。')
    if not range_mode and (args.retry_errors or args.refresh_source):
        parser.error('--retry-errors 和 --refresh-source 用于范围模式')
    if not range_mode and (args.full_speed or args.download_workers is not None):
        parser.error('--full-speed 和 --download-workers 用于范围模式')
    if not range_mode and args.pipeline is not None:
        parser.error('--pipeline 用于范围模式')
    if args.share_computations and (not range_mode or args.pipeline=='legacy'):
        parser.error('--share-computations 仅用于staged范围流水线，不能与legacy组合')
    if args.compact and (not range_mode or args.pipeline=='legacy'):
        parser.error('--compact 仅用于staged范围流水线，不能与legacy组合')
    if args.remember_source and not args.compact:
        parser.error('--remember-source 需要 --compact')
    if args.buffer_mib < 32:
        parser.error('--buffer-mib 至少为32，以容纳单响应上限')
    if args.refresh_source and args.source_revision:
        parser.error('--refresh-source 与显式 --source-revision 不能同时使用')
    last_print = [0.0]
    def progress(event):
        now = time.perf_counter()
        if now-last_print[0] > 0.2 or event['processed'] == event['total']:
            eta = event.get('eta_seconds')
            extra = f"，请求 {event['requests']}，{event.get('items_per_second', 0):.2f}项/秒，预计剩余 {eta:.1f}秒" if eta is not None else ''
            print(f"已处理 {event['processed']}/{event['total']}，复用结果 {event['resumed']}，失败 {event['errors']}{extra}", flush=True)
            last_print[0] = now
    lease = CacheLease(args.state_dir)
    try:
        lease.__enter__()
        from .storage_profile import require_profile
        require_profile(args.state_dir,compact=args.compact)
        if range_mode:
            validate_range(args.start, args.end)
            endpoint = args.endpoint or QLOGO_ENDPOINT
            rate = args.rate if args.rate is not None else (0 if args.full_speed else 2)
            # Validate all network options before changing the active cache batch.
            source_name = args.source_name or QLOGO_SOURCE_NAMES.get(endpoint, '获授权测试接口')
            SourceConfig(endpoint, args.source_revision or 'validation', name=source_name, token_env=args.token_env,
                         timeout=args.timeout, retries=args.retries, rate=rate)
            revision = args.source_revision or cache_batch(args.state_dir, endpoint, args.token_env, refresh=args.refresh_source)
            config = SourceConfig(endpoint, revision, name=source_name, token_env=args.token_env,
                                  timeout=args.timeout, retries=args.retries, rate=rate, keep_alive=args.full_speed)
            downloads = args.download_workers or (min(256, args.workers*4) if args.max_resources
                else min(128,max(16,(os.cpu_count() or 1)*4)) if args.full_speed else args.workers)
            backend = args.compute_backend or ('process' if args.max_resources else 'thread')
            if args.full_speed:
                retention='完成区间、保留候选和错误持续写入磁盘' if args.compact else '完整记录持续写入磁盘'
                print(f'全速模式：下载并行 {downloads}，计算上限 {args.workers}（{backend}），速率设置 {rate}（0为不固定等待）；{retention}。', flush=True)
            route = search_range
            extra = {}
            if args.compact or args.share_computations or args.pipeline == 'staged' or (args.pipeline is None and args.full_speed):
                from .staged import search_staged
                route = search_staged
                extra['buffer_bytes'] = args.buffer_mib*1024*1024
                extra['share_computations'] = args.share_computations
                extra['compact'] = args.compact
                extra['capture_source'] = args.remember_source
            result = route(args.target, args.start, args.end, args.state_dir, config, workers=args.workers,
                                  progress=progress, retry_errors=args.retry_errors, download_workers=downloads,
                                  stream_results=args.full_speed, top_limit=max(1, min(args.limit, 100)),
                                  shared_writer=args.max_resources, compute_backend=backend, **extra)
        else:
            result = search(args.target, args.folder, args.state_dir, workers=args.workers, progress=progress)
        if args.note:
            result['dataset_note'] = args.note
        reports = write_reports(result, args.output, limit=max(1, min(args.limit, 100)))
        print(json.dumps({'status': result['status'], 'stats': result['stats'], 'report': str(reports['html'])}, ensure_ascii=False, indent=2))
        if args.open_report:
            webbrowser.open(reports['html'].as_uri())
        return (1 if range_mode and result['stats']['errors'] else 0) if result['status'] == 'complete' else 130
    except Exception as error:
        print(f'无法完成比较：{type(error).__name__}: {error}', file=sys.stderr)
        return 2
    finally:
        lease.close()


if __name__ == '__main__':
    raise SystemExit(main())
