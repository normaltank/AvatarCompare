"""One-click, hardware-sized ceiling measurement; synthetic data on loopback only."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone, timedelta
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys


def summarize(output):
    lines = ['本机极限测速结果（仅合成素材和本机回环接口，不是公网速度）', '']
    for folder, label in [('end-to-end', '完整流程：线程提取、获取、比对、持久化'),
                          ('end-to-end-process', '完整流程：多进程提取，包含进程启动'),
                          ('process-compute', '多进程纯计算：不含下载、持久化及进程启动')]:
        records = json.loads((output/folder/'resources.json').read_text(encoding='utf-8'))['samples']
        groups = {}
        for row in records:
            groups.setdefault((row['workers'], row.get('downloads')), []).append(row)
        lines.append(label)
        best = None
        for (cpu, downloads), rows in groups.items():
            rate = statistics.median(row['items_per_second'] for row in rows)
            average = statistics.median(row['resources']['process_cpu_percent'] for row in rows)
            memory = max(row['resources']['peak_private_bytes'] for row in rows)/1024**2
            lines.append(f'计算并发{cpu} / 下载并发{downloads or "—"}：中位{rate:.2f}项/秒，CPU平均{average:.1f}%，私有内存峰值{memory:.0f}MiB')
            if best is None or rate > best[0]:
                best = (rate, cpu, downloads)
        description = f'{best[1]}计算 / {best[2]}下载' if best[2] else f'{best[1]}计算进程（不含下载）'
        lines.append(f'本轮最高中位吞吐：{best[0]:.2f}项/秒，配置{description}。')
        lines.append('')
    lines.extend(['CPU百分比按整机逻辑CPU数归一化；多进程统计包含计算子进程。',
                  '完整流程含本机模拟服务自身开销；纯计算不能代表实际任务速度。',
                  '当前SIFT后端为CPU，没有GPU测速结果；私有内存指标为提交量，不等同于物理驻留内存。',
                  '完整原始样本、进程启动耗时、资源采样及错误计数见各子目录resources.json。'])
    (output/'测速结果.txt').write_text('\n'.join(lines), encoding='utf-8-sig')
    print('\n'.join(lines), flush=True)


def main():
    parser = argparse.ArgumentParser(description='全部CPU与高I/O并发阶梯压测，自动输出最高实测速度和资源利用率')
    stamp = datetime.now(timezone(timedelta(hours=8))).strftime('%Y%m%d-%H%M%S')
    parser.add_argument('--output', type=Path, default=Path('outputs')/('limit-suite-'+stamp))
    parser.add_argument('--count', type=int, default=512)
    parser.add_argument('--repeats', type=int, default=3)
    args = parser.parse_args()
    cpus = min(os.cpu_count() or 1, 256)
    levels = sorted({value for value in [2, 4, 8, 16, 32, 64, cpus] if value <= cpus})
    configurations = [(n, min(256, max(16, n*4))) for n in levels]
    extra = (cpus, min(256, cpus*8))
    if extra not in configurations:
        configurations.append(extra)
    process_levels = [n for n in levels if n >= 8] or [cpus]
    probe = Path(__file__).with_name('resource_probe.py')
    output = args.output.resolve()
    print(f'检测到{cpus}个逻辑CPU；逐档测试至全部逻辑CPU及最多{configurations[-1][1]}路下载。', flush=True)
    print('此压测会短时间大量使用本机CPU；所有图片为合成素材，接口仅监听本机。', flush=True)
    commands = [
        [sys.executable, str(probe), '--output', str(output/'end-to-end'), '--count', str(args.count),
         '--repeats', str(args.repeats), '--staged', '--shared-writer', '--configs', ','.join(f'{c}:{d}' for c,d in configurations)],
        [sys.executable, str(probe), '--output', str(output/'end-to-end-process'), '--count', str(args.count),
         '--repeats', str(args.repeats), '--staged', '--shared-writer', '--process-extract', '--configs', f'{cpus}:{min(256,cpus*4)}'],
        [sys.executable, str(probe), '--output', str(output/'process-compute'), '--count', '8192',
         '--repeats', '2', '--process-compute', '--configs', ','.join(f'{n}:1' for n in process_levels)]
    ]
    for command in commands:
        completed = subprocess.run(command)
        if completed.returncode:
            return completed.returncode
    summarize(output)
    print('记录目录：'+str(output), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
